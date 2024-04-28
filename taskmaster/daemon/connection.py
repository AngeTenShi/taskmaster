from .daemon import *
from .decorators import no_sigchld

import pickle
import socket
import select
import os

class ClientConnection():
	def __init__(self, daemon, client_id, conn, addr):
		self.daemon : Daemon = daemon
		self.id = client_id

		self.conn : socket.socket = conn
		self.addr = addr

		self.recv_buf = bytearray()
		self.recv_state = "SIZE"
		self.recv_size = 4

		self.send_buf = bytearray()

	def disconnect(self):
		self.conn.close()
		self.conn = None

	def __del__(self):
		if self.conn:
			self.disconnect()

	def recv(self):
		if not self.conn:
			return

		r = self.conn.recv(1024)
		if not len(r):
			raise Exception("Connection closed")
		
		self.recv_buf += r

		data_size = 0
		while len(self.recv_buf) >= self.recv_size:
			cmd = None
			data_size = self.recv_size

			if self.recv_state == "SIZE":
				self.recv_state = "PAYLOAD"
				self.recv_size = int.from_bytes(self.recv_buf[:data_size], "little")

			elif self.recv_state == "PAYLOAD":
				self.recv_state = "SIZE"
				self.recv_size = 4
				cmd = pickle.loads(self.recv_buf[:data_size])

			self.recv_buf = self.recv_buf[data_size:]

			if cmd:
				self.daemon.command_queue.put_nowait((self.id, cmd))

	def prepare_send(self, buf):
		self.send_buf += buf

	def send(self):
		if not self.conn:
			return

		if len(self.send_buf):
			len_sent = self.conn.send(self.send_buf)
			if not len_sent:
				raise Exception("Connection closed")
			self.send_buf = self.send_buf[len_sent:]

class SocketServer():
	def __init__(self, daemon):
		self.server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		self.daemon = daemon

		# Try deleting the socket if it already exists, before creating it
		try:
			os.unlink("/tmp/taskmaster.sock")
		except OSError:
			pass

		# Await connections on the socket
		self.server_sock.bind("/tmp/taskmaster.sock")
		self.server_sock.listen(3)

		self.client_id = 0
		self.clients_by_sock = {}


	def __del__(self):
		self.server_sock.close()
		os.unlink("/tmp/taskmaster.sock")

	def add_client(self, conn, addr):
		self.clients_by_sock[conn] = ClientConnection(self.daemon, self.client_id, conn, addr)
		self.client_id += 1
	
	def remove_client(self, client):
		del self.clients_by_sock[client.conn]
		client.disconnect()

	def get_client_by_id(self, client_id):
		return next((c for c in self.clients_by_sock.values() if c.id == client_id), None)

	def get_client_by_sock(self, sock):
		return self.clients_by_sock.get(sock, None)

	def get_socks(self):
		return [c.conn for c in self.clients_by_sock.values()]

	def binarize_responses(self):
		while self.daemon.output_queue.qsize() > 0:
			client_id, response = self.daemon.output_queue.get_nowait()

			c = self.get_client_by_id(client_id)
			if c:
				data = pickle.dumps(response)
				size = len(data).to_bytes(4, "little")
				c.prepare_send(size + data)

			self.daemon.output_queue.task_done()
		
	def handle_recvs(self, r):
		for sock in r:
			if sock == self.server_sock:
				self.add_client(*self.server_sock.accept())

			else:
				client = self.get_client_by_sock(sock)
				if client:
					try:
						client.recv()
					except:
						self.remove_client(client)
						return False

		return True
	
	def handle_sends(self, w):
		for sock in w:
			client = self.get_client_by_sock(sock)
			if client:
				try:
					client.send()
				except:
					self.remove_client(client)
					return False
		return True


	def loop(self):
		while True:
			# Prepare responses to be sent to clients
			self.binarize_responses()

			# Handle all clients and new connections
			socks = self.get_socks()
			r, w, __ = select.select([self.server_sock] + socks, socks, [], 1)

			# Handle recvs and sends, if any client disconnected, we need to restart the loop, to not process events of old clients
			if not self.handle_recvs(r):
				continue 	
			if not self.handle_sends(w):
				continue

@no_sigchld
def socket_thread(daemon):
	"""
	This is the entry point for the socket server thread, the one responsible of handling the connections with the clients, populating and consuming the queues
	"""
	try:
		server = SocketServer(daemon)
		server.loop()
	except KeyboardInterrupt:
		daemon.logger.info("SOCKET SERVER INTERRUPTED")
	except Exception as e:
		daemon.logger.info("SOCKET SERVER ERROR: ", e)
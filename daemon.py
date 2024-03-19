from common import CommandRequest, CommandResponse, CommandType, CommandQueue
from typing import List, Dict
from enum import Enum

from collections import defaultdict

import signal
import os
import json
import threading
import pickle
import socket
import sys
import subprocess
import select

from queue import Queue

class Status(Enum):
	STOPPED = 1
	STARTING = 2
	RUNNING = 3
	BACKOFF = 4
	STOPPING = 5
	EXITED = 6
	FATAL = 7
	UNKNOWN = 8

class Daemon:
	def __init__(self):
		self.command_queue = Queue(maxsize=30)
		self.output_queue = Queue(maxsize=30)
		self.config = None
		self.programs : Dict[str, List[Program]] = {}

class Program:
	def __init__(self, name : str, config):
		self.name = name
		self.config = config
		self.pid = None
		self.exit_code = None
		self.start_retries = 0
		self.status = Status.STOPPED
		self.exit_timer = None
		self.start_timer = None

	def should_auto_restart(self, exitcode):
		return self.config["autorestart"] == True or (self.config["autorestart"] == "unexpected" and exitcode not in self.config["exitcodes"])

	def set_running(self):
		self.status = Status.RUNNING

	def clear(self):
		self.pid = None
		self.exit_code = None
		self.start_retries = 0
		self.status = Status.STOPPED
		if self.exit_timer:
			self.exit_timer.cancel()
		self.exit_timer = None
		if self.start_timer:
			self.start_timer.cancel()
		self.start_timer = None

daemon = Daemon()

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
		print(f"New client {self.client_id} connected")
		self.client_id += 1
	
	def remove_client(self, client):
		del self.clients_by_sock[client.conn]
		client.disconnect()
		print(f"Client {client.id} disconnected")

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

			daemon.output_queue.task_done()
		
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
			self.binarize_responses()

			# Handle all clients and new connections
			socks = self.get_socks()
			r, w, __ = select.select([self.server_sock] + socks, socks, [], 1)

			# Handle recvs and sends, if any client disconnected, we need to restart the loop, to not process events of old clients
			if not self.handle_recvs(r):
				continue 	
			if not self.handle_sends(w):
				continue

def socket_thread(daemon):

	try:
		server = SocketServer(daemon)
		server.loop()
	except KeyboardInterrupt:
		print("SOCKET SERVER INTERRUPTED")
	except Exception as e:
		print("SOCKET SERVER ERROR: ", e)

def at_least_one_arg(f):
	"""
	Decorator for commands that require at least one argument
	"""
	global daemon

	def wrapper(command_args):
		if len(command_args) > 0:
			return False, f(daemon, command_args)
		return False, "Invalid args"
	return wrapper

def one_arg(f):
	"""
	Decorator for commands that only require one argument
	"""
	global daemon

	def wrapper(command_args):
		if len(command_args) == 1:
			return False, f(daemon, *command_args)
		return False, "Invalid args"
	return wrapper

def no_arg(f):
	"""
	Decorator for commands that don't require any arguments
	"""
	global daemon

	def wrapper(command_args):
		if len(command_args) == 0:
			return False, f(daemon)
		return False, "Invalid args"
	return wrapper


@no_arg
def status(d: Daemon):
	"""
	Shows the status of all the programs
	"""
	return {name: [proc.status.name for proc in procs] for name, procs in d.programs.items()}

@at_least_one_arg
def start_program(d: Daemon, programs: List[str]):
	"""
	Starts whole programs (all processes)
	"""
	ret = {}
	# remove duplicates in programs ex : start ls ls should only start ls
	programs = list(set(programs))
	for to_start in programs:
		ret[to_start] = {"starting": False}
		if to_start not in d.programs:
			continue
		if not all([proc.status == Status.STOPPED for proc in d.programs[to_start]]):
			continue
		if any(proc.status == Status.STARTING or proc.status == Status.RUNNING for proc in d.programs[to_start]):
			continue
		for proc in d.programs[to_start]:
			d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [proc], -1)))
		ret[to_start]["starting"] = True
	return ret


@one_arg
def stop_program(d: Daemon, programs: List[str]):
	"""
	Stops whole programs (all processes)
	"""
	ret = {}
	for to_stop in programs:
		ret[to_stop] = {"stopping": False}
		if not any(proc.status == Status.STOPPING for proc in d.programs[to_stop]):
			continue
		elif all(proc.status == Status.STOPPED for proc in d.programs[to_stop]):
			continue
		for proc in d.programs[to_stop]:
			d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_STOP_PROC, [proc], -1)))
		ret[to_stop]["stopping"] = True
	return ret

@at_least_one_arg
def restart_program(d: Daemon, programs: List[str]):
	"""
	Restarts a program
	"""
	ret = {}
	for to_restart in programs:
		ret[to_restart] = {"restarting": False}
		if to_restart not in d.programs:
			print(f"Program {to_restart} not found")
			continue
		if any(proc.status == Status.STARTING for proc in d.programs[to_restart]):
			print(f"Program {to_restart} is already starting")
			continue
		for proc in d.programs[to_restart]:
			d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [proc], -1)))
		ret[to_restart]["restarting"] = True
	return ret


@at_least_one_arg
def reload_config(d: Daemon, config_content: str):
	"""
	Reloads the configuration
	"""
	# One already running, we need to stop everything first
	# TODO: Figure out how to wait for the stop before restarting
	if d.config != None:
		print("Reloading config and stopping everything first")
		stop_program(list(d.config["programs"].keys()))
	d.config = json.loads(config_content[0])
	d.programs = defaultdict(list)
	for program_name, program_config in d.config["programs"].items():
		for _ in range(program_config["numprocs"]):
			print(f"Creating program {program_name}")
			d.programs[program_name].append(Program(program_name, program_config))
	d.command_queue.put_nowait((-1, CommandRequest(CommandType.START_PROGRAM, [name for name, conf in d.config["programs"].items() if conf["autostart"] == True], -1)))
	return "Config reloaded"

@one_arg
def internal_start_proc(d: Daemon, program: Program):
	"""
	Start a program "proc"
	"""
	if program.status != Status.STOPPED and program.status != Status.BACKOFF:
		os.kill(program.pid, signal.SIGKILL)
	old_umask = os.umask(0)
	if "umask" in program.config:
		os.umask(program.config["umask"])

	#TODO: Fix bug where the timer happens anyway, and programs go RUNNING after startsecs, even when in FATAL/EXITED
	with open(program.config["stdout"], "w") as stdout, open(program.config["stderr"], "w") as stderr:
		try:
			process = subprocess.Popen(
				program.config["cmd"].split(" "),
				cwd = program.config["workingdir"],
				stdin = subprocess.DEVNULL,
				stdout=stdout,
				stderr=stderr,
				env = os.environ.copy() | (program.config["env"] if "env" in program.config else {}))

			program.pid = process.pid
			program.status = Status.STARTING
			program.start_retries += 1
			print(f"program {program.pid}: {program.status} will start", program.start_retries, program.config["startretries"], file=sys.stderr)
			if program.config["starttime"] != 0:
				program.start_timer = threading.Timer(program.config["starttime"], lambda: program.set_running())
				program.start_timer.start()
			else:
				program.set_running()
		except Exception as e:
			print("failed to even subprocess.Popen(): ", e)
			pass


	os.umask(old_umask)


@one_arg
def internal_stop_proc(d: Daemon, program: Program):
	"""
	Stop a program "proc"
	"""
	signal = getattr(signal, f"SIG{program.config['stopsignal']}")

	os.kill(program.pid, signal)
	program.status = Status.STOPPING
	program.exit_timer = threading.Timer(program.config["stoptime"], lambda: os.kill(program.pid, signal.SIGKILL))
	program.exit_timer.start()

def handle_sigchld(d: Daemon):
	"""
	Handles the stop a process managed by the daemon, task to restart is scheduled to make sure this function executes as fast as possible.
	Maybe make it smaller if it keeps missing SIGCHLD?
	"""
	stopped = []

	#print("SIGCHLD received")
	# Wait all of processes that stopped
	while True:
		try:
			pid, _ = os.waitpid(-1, os.WNOHANG)
			if (pid, _) == (0, 0):
				break
			stopped.append((pid, os.WEXITSTATUS(_)))
		except:
			break
	#print(f"Pid : {pid}, exit code : {_}")

	# Restart handling for all of them
	for pid, exit_code in stopped:
		# Find the right program
		elprograma : Program = None
		for program_list in d.programs.values():
			for proc in program_list:
				if proc.pid == pid:
					elprograma = proc
					break

		#print(f"Program {elprograma.name} exited with code {exit_code}", file=sys.stderr)
		#print(f"Start retries: {elprograma.start_retries} , Statuts : {elprograma.status}", file=sys.stderr)
		# Handle what needs to be done next after its stop
		if elprograma != None:
			config = elprograma.config

			# It was tried to be started, did not run for long enough to be considered running
			if elprograma.status == Status.STARTING:
				# We should restart it if it did not exceed startretries
				if elprograma.start_retries < config["startretries"]:
					print(f"program {elprograma.pid}: {elprograma.status} will restart", elprograma.start_retries, config["startretries"], file=sys.stderr)
					elprograma.status = Status.BACKOFF
					d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [elprograma], -1)))
					return 
				else:
					elprograma.clear()
					elprograma.status = Status.FATAL
					elprograma.exit_code = exit_code
					return

			# It was running and exited itself
			elif elprograma.status == Status.RUNNING:
				elprograma.clear()
				elprograma.status = Status.EXITED
				elprograma.exit_code = exit_code
				if elprograma.should_auto_restart(exit_code):
					d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [elprograma], -1)))

			# It was running and we stopped it
			elif elprograma.status == Status.STOPPING:
				elprograma.clear()

def daemon_loop(daemon: Daemon):
	commands = {
		CommandType.RELOAD_CONFIG: reload_config,
		CommandType.START_PROGRAM: start_program,
		CommandType.STOP_PROGRAM: stop_program,
		CommandType.RESTART_PROGRAM: restart_program,
		CommandType.INTERNAL_START_PROC: internal_start_proc,
		CommandType.INTERNAL_STOP_PROC: internal_stop_proc,
		CommandType.STATUS: status,
		CommandType.ABORT: lambda: (True, "")
	}

	abort = False
	while not abort:
		client_id, cmd = daemon.command_queue.get()
		if cmd.cmd_type not in commands.keys():
			continue
		print(f"Received command {cmd.cmd_type.name} with args {cmd.args}")
		abort, response = commands[cmd.cmd_type](cmd.args)
		if client_id != -1 and cmd.id != -1: # Internal ID used for commands issued by the daemon itself
			daemon.output_queue.put_nowait((client_id, CommandResponse(cmd.cmd_type, response, cmd.id)))
		daemon.command_queue.task_done()


def daemon_entry():
	"""
	This is the entry point for the daemon thread, the one responsible of scheduling and handling the processes.
	It receives the configuration and orders from the main thread and performs accordingly
	"""
	# Create pid file
	with open("/tmp/taskmaster.pid", "w") as f:
		f.write(str(os.getpid()))

	# Initialize signals
	signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))
	socket_th = threading.Thread(target=socket_thread, args=(daemon,))
	socket_th.start()

	# Process commands
	daemon_loop(daemon)

	socket_th.join()

def check_and_print_if_already_running():
	"""
	Checks if the daemon is already running
	"""
	try:
		with open("/tmp/taskmaster.pid", "r") as f:
			pid = int(f.read())
			print(f"Taskmaster daemon already running (pid={pid}), exiting...", file=sys.stderr)
			return True
	except:
		return False

from daemonize import Daemonize
import logging

if __name__ == "__main__":
	#th = Thread(target=daemon_entry, daemon=True)
	#th.start()
	#th.join()
	"""
	logger= logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)
	logger.propagate = False
	fh = logging.FileHandler('/tmp/daemon.log')
	fh.setLevel(logging.DEBUG)
	logger.addHandler(fh)
	daemon = Daemonize(app="bigniggaballs",action=daemon_entry, pid='/tmp/mydaemon.pid')
	daemon.start()
	"""
	# Daemonize(app="supervisord", action=daemon_entry, pid='/tmp/daemon.pid').start()
	if not check_and_print_if_already_running():
		try:
			daemon_entry()
		except Exception as e:
			print(e)
			pass
		finally:
			os.unlink("/tmp/taskmaster.pid")
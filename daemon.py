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
import logging
import time 
import traceback
from multiprocessing import Event

from queue import Queue, PriorityQueue

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
		self.logger = None
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
		self.process = None
		self.stack = None

	def copy(self):
		p = Program(self.name, self.config)
		p.pid = self.pid
		p.exit_code = self.exit_code
		p.start_retries = self.start_retries
		p.status = self.status
		p.exit_timer = self.exit_timer
		p.start_timer = self.start_timer
		p.process = self.process
		p.stack = self.stack
		return p
	def should_auto_restart(self, exitcode):
		return self.config["autorestart"] == True or (self.config["autorestart"] == "unexpected" and exitcode not in self.config["exitcodes"])

	def set_running(self):
		self.status = Status.RUNNING

	def set_pid(self, pid):
		self.pid = pid

	def set_process(self, process):
		self.process = process

	def clear(self):
		self.pid = None
		self.exit_code = None
		#self.start_retries = 0
		self.status = Status.STOPPED
		if self.exit_timer:
			self.exit_timer.cancel()
		self.exit_timer = None
		if self.start_timer:
			self.start_timer.cancel()
		# self.start_timer = None

daemon = Daemon()
logger = logging.getLogger('my_daemon')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('daemon.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.info('Daemon started')
daemon.logger = logger

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
	signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGCHLD]) 
 
	try:
		server = SocketServer(daemon)
		server.loop()
	except KeyboardInterrupt:
		daemon.logger.info("SOCKET SERVER INTERRUPTED")
	except Exception as e:
		daemon.logger.info("SOCKET SERVER ERROR: ", e)


### TIMED EVENTS ##
class ScheduledEvent:
	def __init__(self, ts, func):
		self.ts = ts
		self.func = func
		self.defused = False

	def __lt__(self, other):
		return self.ts < other.ts

	def timestamp(self):
		return self.ts

	def cancel(self):
		self.defused = True

	def exec(self):
		if not self.defused:
			self.func()

has_new_event = threading.Event()
new_event = None

def schedule_event(s, func):
	t = threading.Timer(s, func)
	t.start()
	return t
	global new_event
	global has_new_event

	e = ScheduledEvent(time.time() + s, func)
	has_new_event.set()
	new_event = e
	return e

# TODO: Test this
def scheduler_thread():
	global new_event
	global has_new_event

	signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGCHLD]) 

	evs = PriorityQueue()	

	# Wait for every event to happen and perform its logic, off by miliseconds errors are OK here.
	while True:
		most_recent = None

		# Compute the time for next event
		if not evs.empty():
			# TODO: Protect this, from being written to and read from at the same time
			most_recent = evs.queue[0].timestamp() - time.time()

		# Wait for next event or new event
		try:
			has_timeout = not has_new_event.wait(most_recent)
		except ValueError:
			# If we tried to sleep negative time, it means we have something to do instantly, which means we can apply the `timeout` logic
			has_timeout = True

		# Execute the most recent event
		if has_timeout:
			e = evs.get_nowait()
			e.exec()
			evs.task_done()
		# New event, add it to the queue
		else:
			has_new_event.clear()
			evs.put_nowait(new_event)
			new_event = None


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

def block_signals(siglist):
	def decorator(f):
		def wrapper(*args, **kwargs):
			signal.pthread_sigmask(signal.SIG_BLOCK, siglist)
			signal.signal(signal.SIGCHLD, signal.SIG_IGN)
			res = f(*args, **kwargs)
			signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))
			signal.pthread_sigmask(signal.SIG_UNBLOCK, siglist)
			return res
		return wrapper
	return decorator

@no_arg
def status(d: Daemon):
	"""
	Shows the status of all the programs
	"""
	return {name: [(id(proc), proc.status.name) for proc in procs] for name, procs in d.programs.items()}

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
	d.logger.info(programs)
	for to_restart in programs:
		ret[to_restart] = {"restarting": False}
		if to_restart not in d.programs:
			d.logger.info(f"Program {to_restart} not found")
			continue
		if any(proc.status == Status.STARTING for proc in d.programs[to_restart]):
			d.logger.info(f"Program {to_restart} is already starting")
			continue
		d.logger.info(f"Program {to_restart} is restarting and programs are {d.programs}")
		for proc in d.programs[to_restart]:
			d.logger.info(f"Restarting program {to_restart}")
			d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [proc], -1)))
		ret[to_restart]["restarting"] = True
	return ret


@at_least_one_arg
def reload_config(d: Daemon, config_content: str):
	"""
	Reloads the configuration
	"""
	# One already running, we need to stop everything first
	if d.config != None:
		d.logger.info("Reloading config and stopping everything first")
		stop_program(list(d.config["programs"].keys()))
	d.config = json.loads(config_content[0])
	d.programs = defaultdict(list)
	for program_name, program_config in d.config["programs"].items():
		for _ in range(program_config["numprocs"]):
			d.logger.info(f"Creating program {program_name}")
			d.programs[program_name].append(Program(program_name, program_config))
	d.command_queue.put_nowait((-1, CommandRequest(CommandType.START_PROGRAM, [name for name, conf in d.config["programs"].items() if conf["autostart"] == True], -1)))
	return "Config reloaded"

@one_arg
@block_signals([signal.SIGCHLD])
def internal_start_proc(d: Daemon, program: Program):
	"""
	Start a program "proc"
	"""
	os.write(1, bytes(f"in internal start proc BEGIN\n", "utf-8"))
	# os.write(1, bytes(f"in internal start proc {program.stack}\n", "utf-8"))
	if program is None:
		return
	if program.status is not None:
		if program.status != Status.BACKOFF and program.status != Status.STOPPED and program.status != Status.EXITED and program.status != Status.FATAL:
			try:
				os.write(1, bytes(f"killing {id(program)} {program.status}\n", "utf-8"))
				os.kill(program.pid, signal.SIGKILL)
			except:
				pass

	old_umask = os.umask(0)
	if "umask" in program.config:
		os.umask(program.config["umask"])

	try:
		with open(program.config["stdout"], "a") as stdout, open(program.config["stderr"], "a") as stderr:
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
				program.set_process(process)

				if program.config["starttime"] != 0:
					program.start_timer = schedule_event(program.config["starttime"], lambda: program.set_running())
				else:
					program.set_running()
			except Exception as e:
				program.status = Status.FATAL
				print("failed to even subprocess.Popen()")

	except Exception as e:
		program.status = Status.FATAL
		print("failed to open stdout/stderr")

	os.umask(old_umask)
	os.write(1, bytes(f"in internal start proc END\n", "utf-8"))


@one_arg
def internal_stop_proc(d: Daemon, program: Program):
	"""
	Stop a program "proc"
	"""
	signal = getattr(signal, f"SIG{program.config['stopsignal']}")

	os.kill(program.pid, signal)
	program.status = Status.STOPPING
	program.exit_timer = schedule_event(program.config["stoptime"], lambda: os.kill(program.pid, signal.SIGKILL))

def handle_sigchld(d: Daemon):
	"""
	Handles the stop a process managed by the daemon, task to restart is scheduled to make sure this function executes as fast as possible.
	Maybe make it smaller if it keeps missing SIGCHLD?
	"""
	os.write(1, bytes("SIGCHLD\n", "utf-8"))
	stopped = []

	# Wait all of processes that stopped
	count = 0
	
	for program in d.programs.values():
		for proc in program:
			proc.process.poll()
			if proc.process.returncode is not None:
				elprograma = proc
				exit_code = elprograma.process.returncode
				if elprograma != None:
					# os.write(1, bytes(f"found in programs ({id(elprograma)})\n", "utf-8"))
					elprograma.start_timer.cancel()
					config = elprograma.config
					# It was tried to be started, did not run for long enough to be considered running
					if elprograma.status == Status.STARTING:
						# We should restart it if it did not exceed startretries
						if elprograma.start_retries < config["startretries"]:
							#d.logger.info(f"program {elprograma.pid}: {elprograma.status} will restart")
							elprograma.status = Status.BACKOFF
							os.write(1, bytes(f"program {id(elprograma)} was {elprograma.status}\n", "utf-8"))
							elprograma.stack = "".join(traceback.format_stack())
							new_program = elprograma.copy()
							d.programs[elprograma.name].append(new_program)
							time.sleep(0.2)
							schedule_event(elprograma.start_retries, lambda: d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [new_program], -1))))
							d.programs[elprograma.name].remove(elprograma)
							elprograma.clear()
						else:
							elprograma.clear()
							elprograma.status = Status.FATAL
							elprograma.exit_code = exit_code

					# It was running and exited itself
					elif elprograma.status == Status.RUNNING:
						elprograma.clear()
						elprograma.status = Status.EXITED
						elprograma.exit_code = exit_code
						if elprograma.should_auto_restart(exit_code):
							os.write(1, bytes(f"program {elprograma.pid} was {elprograma.status}\n", "utf-8"))
							elprograma.stack = "".join(traceback.format_stack())
							d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [elprograma], -1)))

					# It was running and we stopped it
					elif elprograma.status == Status.STOPPING:
						elprograma.clear()
				else:
					os.write(1, bytes(f"was not found in programs\n", "utf-8"))

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

	# Start socket thread
	socket_th = threading.Thread(target=socket_thread, args=(daemon,))
	socket_th.start()

	# Start scheduler
	scheduler = threading.Thread(target=scheduler_thread)
	scheduler.start()

	# Process commands
	daemon_loop(daemon)

	socket_th.join()
	scheduler.join()

def check_and_logger_debug_if_already_running():
	"""
	Checks if the daemon is already running
	"""
	try:
		if (os.path.exists("/tmp/taskmaster.pid")):
			logger.info(f"Taskmaster daemon already running exiting...")
			return True
	except:
		return False

from daemonize import Daemonize

if __name__ == "__main__":
	if not check_and_logger_debug_if_already_running():
		try :
			#d = Daemonize(app="supervisord", action=daemon_entry, pid="/tmp/taskmaster.pid", foreground=True)
			#d.start()
			daemon_entry()
		except Exception as e:
			logger.info("Daemon entry error: ", e)
			pass
		finally : 
			os.unlink("/tmp/taskmaster.pid")
			#d.exit()
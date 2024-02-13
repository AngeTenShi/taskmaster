from common import Command, CommandType, CommandQueue
from typing import List, Dict
from enum import Enum

from collections import defaultdict

import signal
import os
import json
import threading
import pickle
import socket
import select
import subprocess

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
		self.command_queue = Queue(maxsize=10)
		self.output_queue = Queue(maxsize=10)
		self.config = None
		self.programs = Dict[str, List[Program]]

class Program:
	def __init__(self, name : str, config : Dict[str, str]):
		self.name = name
		self.config = config
		self.pid = None
		self.start_time = None
		self.exit_code = None
		self.start_retries = 0
		self.status = Status.STOPPED

	def clear(self):
		self.pid = None
		self.start_time = None
		self.exit_code = None
		self.start_retries = 0
		self.status = Status.STOPPED

daemon = Daemon()

####
"""
	Processus:
		STATE 1: Taille (4 octets)
		STATE 2: Command (taille octets)
"""

def socket_thread(daemon):
	s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	try:
		os.remove("/tmp/daemon.sock")
	except OSError:
		pass
	s.bind("/tmp/daemon.sock")
	fd_set = [s.fileno()]

	read_buf = []
	write_buf = []

	read_state = "SIZE"
	read_size = None
	data = None
	while True:
		r_list, w_list, _ = select.select(fd_set, [], [], 1.0)
		if s.fileno() in r_list:
			try :
				data = s.recv(1024)
			except:
				pass
			if data:
				read_buf.append(data)

				if read_state == "SIZE":
					if len(read_buf) >= 4:
						read_size = int.from_bytes(read_buf[:4], "little")
						read_buf = read_buf[4:]
						read_state = "DATA"

				elif read_state == "DATA":
					if len(read_buf) >= read_size:
						data = pickle.loads(read_buf[:read_size])
						read_buf = read_buf[read_size:]
						read_state = "SIZE"
						daemon.command_queue.put_nowait(data)
		if s.fileno() in w_list:
			while daemon.output_queue.qsize() > 0:
				data = pickle.dumps(daemon.output_queue.get_nowait())
				size = len(data).to_bytes(4, "little")
				write_buf.append(size + data)
				daemon.output_queue.task_done()
			## send data
			sended_size = s.send(write_buf)
			write_buf = write_buf[sended_size:]

def at_least_one_arg(f):
	"""
	Decorator for commands that require at least one argument
	"""
	global daemon

	def wrapper(command_args):
		if len(command_args) > 0:
			f(daemon, command_args)
			return False
	return wrapper

def one_arg(f):
	"""
	Decorator for commands that only require one argument
	"""
	global daemon

	def wrapper(command_args):
		if len(command_args) == 1:
			f(daemon, command_args)
			return False
	return wrapper

def no_arg(f):
	"""
	Decorator for commands that don't require any arguments
	"""
	global daemon

	def wrapper(command_args):
		if len(command_args) == 0:
			f(daemon)
			return False
	return wrapper


@no_arg
def status():
	"""
	Shows the status of all the programs
	"""
	pass

@at_least_one_arg
def start_program(d: Daemon, programs: List[str]):
	"""
	Starts a program
	"""
	for program_list in d.programs.values():
		for program in program_list:
			d.command_queue.put_nowait(Command(CommandType.INTERNAL_START_PROC, program))


@at_least_one_arg
def stop_program(d: Daemon, programs: List[str]):
	"""
	Stops a program
	getit = getattr(signal, f"SIG{c['stopsignal']}")
	"""
	for program_list in d.programs.values():
		for program in program_list:
			d.command_queue.put_nowait(Command(CommandType.INTERNAL_STOP_PROC, program))

@at_least_one_arg
def restart_program(d: Daemon, programs: List[str]):
	"""
	Restarts a program
	"""
	stop_program(d, programs)
	start_program(d, programs)

@one_arg
def reload_config(d: Daemon, config_content: str):
	"""
	Reloads the configuration
	"""
	# One already running, we need to stop everything first
	if d.config != None:
		# TODO: Stop all programs, make sure they are not restarted by the sigchld handler
		stop_program(d, list(d.config.keys()))

	d.config = json.loads(config_content)
	d.programs = {}
	for program_name, config in d.config.items():
		for _ in range(config["numprocs"]):
			d.programs[program_name].append(Program(program_name, config))
	start_program(d, list(d.config["programs"].keys()))

@one_arg
def internal_start_proc(d: Daemon, program: Program):
	"""
	Start a program "proc"
	"""
	old_umask = os.umask(0)
	if "umask" in program.config:
		os.umask(program.config["umask"])

	with open(program.config["stdout"], "w") as stdout, open(program.config["stderr"], "w") as stderr:
		try:
			process = subprocess.Popen(
				program.config["cmd"].split(" "),
				cwd = program.config["workingdir"],
				stdin = subprocess.DEVNULL,
				stdout=stdout,
				stderr=stderr,
				env = program.config["env"])
		except:
			pass

	os.umask(old_umask)


@one_arg
def internal_stop_proc(d: Daemon, program: str):
	"""
	Stop a program "proc"
	"""
	pass

def handle_sigchld(d: Daemon):
	"""
	Handles the stop a process managed by the daemon, task to restart is scheduled to make sure this function executes as fast as possible.
	Maybe make it smaller if it keeps missing SIGCHLD?
	"""
	# Get pid, exit_code, needed informations
	pid, exit_code = os.waitpid(-1, os.WNOHANG)
	elprograma : Program = None
	for program in d.programs:
		for p in program:
			if p.pid == pid:
				elprograma = p
				break


	if elprograma != None:
		# Update our data structures containing our currently running programs
		# Does it have to restart ?
		config = elprograma.config
		if config["autorestart"] == True or (exit_code not in config["exitcodes"] and config["autorestart"] == "unexpected"):
			d.command_queue.put_nowait(Command(CommandType.INTERNAL_START_PROC, elprograma))




def daemon_loop(daemon: Daemon):
	commands = {
		CommandType.RELOAD_CONFIG: reload_config,
		CommandType.START_PROGRAM: start_program,
		CommandType.STOP_PROGRAM: stop_program,
		CommandType.RESTART_PROGRAM: restart_program,
		CommandType.INTERNAL_START_PROC: internal_start_proc,
		CommandType.INTERNAL_STOP_PROC: internal_stop_proc,
		CommandType.STATUS: status,
		CommandType.ABORT: lambda: True
	}

	abort = False
	while not abort:
		cmd = daemon.command_queue.get()
		abort = commands[cmd.id](cmd.args)
		daemon.command_queue.task_done()


def daemon_entry():
	"""
	This is the entry point for the daemon thread, the one responsible of scheduling and handling the processes.
	It receives the configuration and orders from the main thread and performs accordingly
	"""
	# Initialize signals
	# TODO: Make sure the signals arrive to this thread?
	signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))

	socket_th = threading.Thread(target=socket_thread, args=(daemon,));
	socket_th.start()

	# Process commands
	daemon_loop(daemon)

	socket_th.join()

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
	daemon_entry()

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

	# This little hack might have bugs
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

####
"""
	Processus:
		STATE 1: Taille (4 octets)
		STATE 2: Command (taille octets)
"""

class ClientConnection():

	def __init__(self, conn, addr):
		self.conn : socket.socket = conn
		self.conn.setblocking(False)
		self.conn.settimeout(0.1)
		self.addr = addr

		self.recv_buf = bytearray()
		self.recv_state = "SIZE"
		self.recv_size = 4

		self.send_buf = bytearray()

		print(f"client just joined", file=sys.stderr)

	def close(self):
		print(f"client just left", file=sys.stderr)
		self.conn.close()

	def __del__(self):
		print("destructor")
		if self.conn:
			self.close()

	def recv_from_client(self):
		global daemon

		try:
			self.recv_buf += self.conn.recv(1024)
		except:
			pass

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
				daemon.command_queue.put_nowait(cmd)

	def send_to_client(self):
		global daemon

		while daemon.output_queue.qsize() > 0:
			data = pickle.dumps(daemon.output_queue.get_nowait())
			size = len(data).to_bytes(4, "little")
			self.send_buf += size + data
			daemon.output_queue.task_done()

		if len(self.send_buf):
			len_sent = self.conn.send(self.send_buf)
			self.send_buf = self.send_buf[len_sent:]



def socket_thread(daemon):
	s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	try:
		os.unlink("/tmp/daemon.sock")
	except OSError:
		pass
	s.bind("/tmp/daemon.sock")
	s.listen(1)

	# Main loop, accept only one client, and then process its commands, until it disconnects, then do it again
	while True:
		conn = ClientConnection(*s.accept())

		try:
			while True:
				conn.recv_from_client()
				conn.send_to_client()
		except KeyboardInterrupt:
			conn.close()
			break
		#except Exception as e:
		#	print(e)
		#	conn.close()
		#	continue

	s.close()
	os.unlink("/tmp/daemon.sock")

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
			d.command_queue.put_nowait(CommandRequest(CommandType.INTERNAL_START_PROC, [proc], -1))
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
			d.command_queue.put_nowait(CommandRequest(CommandType.INTERNAL_STOP_PROC, [proc], -1))
		ret[to_stop]["stopping"] = True
	return ret

@at_least_one_arg
def restart_program(d: Daemon, programs: List[str]):
	"""
	Restarts a program
	"""
	"""
	for to_restart in programs:
		for proc in d.programs[to_restart]:
			d.command_queue.put_nowait(Command(CommandType.INTERNAL_RESTART_PROC, [proc]))
	"""
	return "Not implemented"

@at_least_one_arg
def reload_config(d: Daemon, config_content: str):
	"""
	Reloads the configuration
	"""
	# One already running, we need to stop everything first
	# TODO: Figure out how to wait for the stop before restarting
	if d.config != None:
		stop_program(list(d.config["programs"].keys()))
	d.config = json.loads(config_content[0])
	d.programs = defaultdict(list)
	for program_name, program_config in d.config["programs"].items():
		for _ in range(program_config["numprocs"]):
			print(f"Creating program {program_name}")
			d.programs[program_name].append(Program(program_name, program_config))
	d.command_queue.put_nowait(CommandRequest(CommandType.START_PROGRAM, [name for name, conf in d.config["programs"].items() if conf["autostart"] == True], -1))
	return "Return value not implemented"

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
				env = os.environ.copy() | (program.config["env"] if "env" in program.config else {}))

			program.pid = process.pid
			program.status = Status.STARTING
			program.start_retries += 1
			if program.config["starttime"] != 0:
				program.start_timer = threading.Timer(program.config["starttime"], lambda: program.set_running())
				program.start_timer.start()
			else:
				program.set_running()
		except Exception as e:
			print(e)
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

stopped = []
def get_all_stopped_pids(d: Daemon):
	global stopped
	while True:
		try:
			pid, _ = os.wait()
			if pid == 0:
				break
			stopped.append((pid, os.WEXITSTATUS(_)))
		except:
			break

def handle_sigchld(d: Daemon):
	"""
	Handles the stop a process managed by the daemon, task to restart is scheduled to make sure this function executes as fast as possible.
	Maybe make it smaller if it keeps missing SIGCHLD?
	"""
	# Wait all of processes that stopped
	global stopped
	th = threading.Thread(target=get_all_stopped_pids, args=(d,))
	th.start()
	th.join() # test if this is blocking
	# Restart handling for all of them
	treated_pid = []
	for pid, exit_code in stopped:
		if pid in treated_pid:
			continue
		# Find the right program
		elprograma : Program = None
		for program_list in d.programs.values():
			for proc in program_list:
				if proc.pid == pid:
					elprograma = proc
					break

		# Handle what needs to be done next after its stop
		if elprograma != None:
			config = elprograma.config
			# It was tried to be started
			if elprograma.status == Status.STARTING:
				elprograma.pid = None
				# We should restart it
				if elprograma.should_auto_restart(exit_code):
					if elprograma.start_retries < config["startretries"]:
						elprograma.status = Status.BACKOFF
						d.command_queue.put_nowait(CommandRequest(CommandType.INTERNAL_START_PROC, [elprograma], -1))
					else:
						elprograma.status = Status.FATAL
						elprograma.exit_code = exit_code
				# Nahh, its over (never even started, negative canthal tilt)
				else:
					elprograma.clear()
					elprograma.status = Status.FATAL
					elprograma.exit_code = exit_code

			# It was running and exited itself
			elif elprograma.status == Status.RUNNING:
				elprograma.clear()
				elprograma.status == Status.EXITED
				elprograma.exit_code = exit_code
				if elprograma.should_auto_restart(exit_code):
					d.command_queue.put_nowait(CommandRequest(CommandType.INTERNAL_START_PROC, [elprograma], -1))

			# We stopped it
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
		cmd = daemon.command_queue.get()
		if cmd.cmd_type not in commands.keys():
			continue
		print(f"Received command {cmd.cmd_type.name} with args {cmd.args}")
		abort, response = commands[cmd.cmd_type](cmd.args)
		if cmd.id != -1: # Internal ID used for commands issued by the daemon itself
			daemon.output_queue.put_nowait(CommandResponse(cmd.cmd_type, response, cmd.id))
		daemon.command_queue.task_done()


def daemon_entry():
	"""
	This is the entry point for the daemon thread, the one responsible of scheduling and handling the processes.
	It receives the configuration and orders from the main thread and performs accordingly
	"""
	# Initialize signals
	signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))
	socket_th = threading.Thread(target=socket_thread, args=(daemon,))
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
	# Daemonize(app="supervisord", action=daemon_entry, pid='/tmp/daemon.pid').start()
	daemon_entry()

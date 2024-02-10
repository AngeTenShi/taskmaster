from common import Command, CommandType, CommandArgs, CommandQueue
from typing import List, Dict

from collections import defaultdict

import signal
import os
import json
import threading
import pickle
import socket

from queue import Queue

class Daemon:
	def __init__(self):
		self.command_queue = Queue(maxsize=10)
		self.output_queue = Queue(maxsize=10)

		self.program_by_pid = {}
		self.pids_by_program = defaultdict(list)
		self.config = None
daemon = Daemon()

####
"""
	Processus:
		STATE 1: Taille (4 octets)
		STATE 2: Command (taille octets)


"""

def socket_thread(daemon):
	socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	socket.connect("/tmp/daemon.sock")
	fd_set = [socket.fileno()]

	read_buf = []
	write_buf = []

	read_state = "SIZE"
	read_size = None
	"""

	"""

	while True:
		r_list, w_list = select.select(fd_set, [], [], 1.0)
		if socket.fileno() in r_list:
			data = socket.recv(1024)
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

		if socket.fileno() in w_list:
			while daemon.output_queue.qsize() > 0:
				data = pickle.dumps(daemon.output_queue.get_nowait())
				size = len(data).to_bytes(4, "little")
				write_buf.append(size + data)
			## send data
			sended_size = socket.send(write_buf)
			write_buf = write_buf[sended_size:]

def at_least_one_arg(f):
	"""
	Decorator for commands that require at least one argument
	"""
	global daemon

	def wrapper(command_args: CommandArgs):
		if len(command_args) > 0:
			f(daemon, command_args)
			return False
	return wrapper

def one_arg(f):
	"""
	Decorator for commands that only require one argument
	"""
	global daemon

	def wrapper(command_args: CommandArgs):
		if len(command_args) == 1:
			f(daemon, command_args)
			return False
	return wrapper

def no_arg(f):
	"""
	Decorator for commands that don't require any arguments
	"""
	global daemon

	def wrapper(command_args: CommandArgs):
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
	pass

@at_least_one_arg
def stop_program(d: Daemon, programs: List[str]):
	"""
	Stops a program
	"""
	pass

@at_least_one_arg
def restart_program(d: Daemon, programs: List[str]):
	"""
	Restarts a program
	"""
	pass

@one_arg
def reload_config(d: Daemon, config_content: str):
	"""
	Reloads the configuration
	"""
	# One already running, we need to stop everything first
	if d.config != None:
		# TODO: Stop all programs, make sure they are not restarted by the sigchld handler
		pass

	d.config = json.loads(config_content)
	pass

@one_arg
def internal_start_proc(d: Daemon, program: str):
	"""
	Start a program "proc"
	"""
	pass

def handle_sigchld(d: Daemon):
	"""
	Handles the stop a process managed by the daemon, task to restart is scheduled to make sure this function executes as fast as possible.
	Maybe make it smaller if it keeps missing SIGCHLD?
	"""
	# Get pid, exit_code, needed informations
	pid, exit_code = os.waitpid(-1, os.WNOHANG)
	program_name = d.program_by_pid.get(pid, None)

	if program_name:
		# Update our data structures containing our currently running programs
		d.pids_by_program[program_name].remove(pid)
		d.program_by_pid.pop(pid)

		# Does it have to restart ?
		config = d.config[program_name]
		if config["autorestart"] == True or (exit_code not in config["exitcodes"] and config["autorestart"] == "unexpected"):
			d.command_queue.put_nowait(Command(CommandType.INTERNAL_START_PROC, [program_name]))




def daemon_loop(daemon: Daemon):
	commands = {
		CommandType.RELOAD_CONFIG: reload_config,
		CommandType.START_PROGRAM: start_program,
		CommandType.STOP_PROGRAM: stop_program,
		CommandType.RESTART_PROGRAM: restart_program,
		CommandType.INTERNAL_START_PROC: internal_start_proc,
		CommandType.STATUS: status,
		CommandType.ABORT: lambda: True
	}

	abort = False
	while not abort:
		cmd = daemon.command_queue.get()
		abort = commands[cmd.id](cmd.args)
		daemon.command_queue.task_done()

	signal.signal(signal.SIGCHLD, signal.SIG_DFL)
	# TODO: Stop all programs


def daemon_entry():
	"""
	This is the entry point for the daemon thread, the one responsible of scheduling and handling the processes.
	It receives the configuration and orders from the main thread and performs accordingly
	"""
	# Initialize signals
	# TODO: Make sure the signals arrive to this thread?
	signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))

	socket_thread = threading.Thread(target=socket_thread, args=(daemon,));
	socket_thread.start()

	# Process commands
	daemon_loop(daemon)

	socket_thread.join()



import select
import socket

# List of Unix domain sockets to monitor
sockets = []

# Add each socket to the list
for i in range(5):
    s = socket.socket(socket.AF_UNIX)
    s.bind('/tmp/mysocket{}'.format(i))
    s.listen()
    sockets.append(s)

# Convert the list of sockets into a sequence of file descriptors
fd_set = [s.fileno() for s in sockets]

# Continuously monitor the sockets for activity
while True:
    rlist, wlist, xlist = select.select(fd_set, [], [], 1.0)

    for fd in rlist:
        s = None
        for sock in sockets:
            if sock.fileno() == fd:
                s = sock
                break

        if s is not None:
            conn, addr = s.accept()
            print('Connected!', addr)


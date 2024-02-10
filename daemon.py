from common import Command, CommandType, CommandArgs, CommandQueue
from typing import List, Dict

from collections import defaultdict

import signal
import os
import json

class Daemon:
	def __init__(self):
		self.command_queue = None
		self.program_by_pid = {}
		self.pids_by_program = defaultdict(list)
		self.config = None
daemon = Daemon()

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


def daemon_entry(q: CommandQueue):
	"""
	This is the entry point for the daemon thread, the one responsible of scheduling and handling the processes.
	It receives the configuration and orders from the main thread and performs accordingly
	"""

	# Initialize daemon data
	global daemon
	daemon.command_queue = q

	# Initialize signals
	# TODO: Make sure the signals arrive to this thread?
	signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))

	# Process commands
	daemon_loop(daemon)
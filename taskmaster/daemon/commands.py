from .daemon import *
from .decorators import block_signals
from ..common import CommandType, CommandRequest, CommandResponse
from . import scheduler

import signal
import subprocess
import os
import json

from typing import List
from collections import defaultdict

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
	return {name: [(id(proc), proc.status.name, proc.start_retries) for proc in procs] for name, procs in d.programs.items()}

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
	if len(programs) == 0:
		return "Invalid program"
	if (type(programs) == str):
		ret[programs] = {"stopping": False}
		for proc in d.programs[programs]:
			if proc.status == Status.STOPPING:
				continue
			if proc.status == Status.STOPPED:
				continue
			d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_STOP_PROC, [proc], -1)))
		ret[programs]["stopping"] = True
		return ret
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
	if config_content[0] is None:
		return "Invalid config file"
	if d.config is not None:
		old_config = d.config
		d.logger.info("Reloading config ...")
		d.config = json.loads(config_content[0])
		for key, value in old_config["programs"].items():
			if value != d.config["programs"].get(key):
				d.logger.info(f"Program {key} has changed")
				d.command_queue.put_nowait((-1, CommandRequest(CommandType.STOP_PROGRAM, [key], -1)))
				if (d.config["programs"].get(key)["autostart"] == True):
					d.command_queue.put_nowait((-1, CommandRequest(CommandType.START_PROGRAM, [key], -1)))
			if key not in d.config["programs"]:
				d.logger.info(f"Program {key} has been removed")
				d.command_queue.put_nowait((-1, CommandRequest(CommandType.STOP_PROGRAM, [key], -1)))
		for key, value in d.config["programs"].items():
			if key not in old_config["programs"]:
				for _ in range(value["numprocs"]):
					d.programs[key].append(Program(key, value))
				d.logger.info(f"Program {key} has been added")
				if (value["autostart"] == True):
					d.command_queue.put_nowait((-1, CommandRequest(CommandType.START_PROGRAM, [key], -1)))
	else:	
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
	os.write(1, bytes(f"in internal start proc {program.name}\n", "utf-8"))
	if program is None:
		return
	if program.status is not None:
		if program.status != Status.BACKOFF and program.status != Status.STOPPED and program.status != Status.EXITED and program.status != Status.FATAL:
			try:
				# ansi_red = "\033[91m"
				# ansi_reset = "\033[0m"
				# os.write(1, bytes(f"{ansi_red}killing {hex(id(program))} {program.status} {ansi_reset}\n", "utf-8"))
				os.kill(program.pid, signal.SIGKILL)
			except Exception as e:
				os.write(1, bytes(f"failed to kill {id(program)} {program.status}\n", "utf-8"))
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
					program.start_timer = scheduler.schedule_event(program.config["starttime"], lambda: program.set_running())
				else:
					program.set_running()
			except Exception as e:
				program.status = Status.FATAL
				print("failed to even subprocess.Popen()", e)

	except Exception as e:
		program.status = Status.FATAL
		print("failed to open stdout/stderr")

	os.umask(old_umask)


@one_arg
def internal_stop_proc(d: Daemon, program: Program):
	"""
	Stop a program "proc"
	"""
	if program is None:
		return
	if program.status == Status.STOPPED:
		return
	if program.status == Status.EXITED:
		return
	if program.status == Status.FATAL:
		return
	if program.status == Status.BACKOFF:
		return
	signale = getattr(signal, f"SIG{program.config['stopsignal']}")

	os.kill(program.pid, signale)
	program.status = Status.STOPPING
	program.exit_timer = scheduler.schedule_event(program.config["stoptime"], lambda: os.kill(program.pid, signal.SIGKILL))
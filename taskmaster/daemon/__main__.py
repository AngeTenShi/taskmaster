from ..common import CommandRequest, CommandResponse, CommandType, CommandQueue

from . import commands, connection, scheduler
from .daemon import *
from .decorators import block_signals

import signal
import os
import threading
import time 
import traceback

@block_signals([signal.SIGCHLD])
def handle_sigchld(d: Daemon):
	"""
	Handles the stop a process managed by the daemon, task to restart is scheduled to make sure this function executes as fast as possible.
	Maybe make it smaller if it keeps missing SIGCHLD?
	"""
	os.write(1, bytes(f"SIGCHLD received\n", "utf-8"))

	elprograma = None

	# Wait all of processes which terminated
	for program in d.programs.values():
		for proc in program:
			#os.write(1, bytes(f"trying proc: {proc}, {hex(id(proc))}\n", "utf-8"))
			# It might not have started yet, so it can be None, by definition
			if proc.process:
				#os.write(1, bytes(f"found proc.process: {proc.process}, {hex(id(proc.process))}\n", "utf-8"))
				exit_code = proc.process.poll()
				# DO NOT restart if not terminated / terminated by a SIGKILL
				if exit_code is not None and exit_code != -signal.SIGKILL:
					elprograma = proc
					if elprograma != None:
						os.write(1, bytes(f"found in programs ({hex(id(proc))})\n", "utf-8"))
						if elprograma.start_timer:
							elprograma.start_timer.cancel()

						config = elprograma.config
						# It was tried to be started, did not run for long enough to be considered running
						if elprograma.status == Status.STARTING:
							os.write(1, bytes(f"program {hex(id(elprograma))} was {elprograma.status}\n", "utf-8"))
							# We should restart it if it did not exceed startretries
							if elprograma.start_retries < config["startretries"]:
								d.logger.info(f"program {elprograma.pid}: {elprograma.status} will restart")
								elprograma.clear()
								elprograma.status = Status.BACKOFF

								# TODO: There is a problem here, try switcing around with proc
								os.write(1, bytes(f"scheduling {hex(id(proc))}\n", "utf-8"))
								def schedule(p):
									scheduler.schedule_event(elprograma.start_retries, lambda: d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [p], -1))))
								schedule(elprograma)
							else:
								elprograma.clear()
								elprograma.status = Status.FATAL
								elprograma.exit_code = exit_code

						#It was running and exited itself
						elif elprograma.status == Status.RUNNING:
							elprograma.clear()
							elprograma.status = Status.EXITED
							elprograma.exit_code = exit_code
							os.write(1, bytes(f"program {elprograma.pid} was {elprograma.status}\n", "utf-8"))
							if elprograma.should_auto_restart(exit_code):
								elprograma.stack = "".join(traceback.format_stack())
								def schedule(p):
									scheduler.schedule_event(1, lambda: d.command_queue.put_nowait((-1, CommandRequest(CommandType.INTERNAL_START_PROC, [p], -1))))
								schedule(elprograma)

						#It was running and we stopped it
						elif elprograma.status == Status.STOPPING:
							os.write(1, bytes(f"program {elprograma.pid} was {elprograma.status}\n", "utf-8"))
							elprograma.clear()
						else:
							os.write(1, bytes(f"program {elprograma.pid} was {elprograma.status}\n", "utf-8"))
					else:
						os.write(1, bytes(f"was not found in programs\n", "utf-8"))
				elif exit_code == -signal.SIGKILL:
					ansi_bold = "\033[1m"
					ansi_red = "\033[91m"
					ansi_reset = "\033[0m"
					os.write(1, bytes(f"{ansi_bold}{ansi_red}PROCESS SIGKILLED WAS NOT RESTARTED{ansi_reset}\n", "utf-8"))

def daemon_loop(daemon: Daemon):
	handlers = {
		CommandType.RELOAD_CONFIG: commands.reload_config,
		CommandType.START_PROGRAM: commands.start_program,
		CommandType.STOP_PROGRAM: commands.stop_program,
		CommandType.RESTART_PROGRAM: commands.restart_program,
		CommandType.INTERNAL_START_PROC: commands.internal_start_proc,
		CommandType.INTERNAL_STOP_PROC: commands.internal_stop_proc,
		CommandType.STATUS: commands.status,
		CommandType.ABORT: lambda: (True, "")
	}

	abort = False
	while not abort:
		client_id, cmd = daemon.command_queue.get()
		if cmd.cmd_type not in handlers.keys():
			continue
		abort, response = handlers[cmd.cmd_type](cmd.args)
		if client_id != -1 and cmd.id != -1: # Internal IDs used for commands issued by the daemon itself
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
	socket_th = threading.Thread(target=connection.socket_thread, args=(daemon,))
	socket_th.start()

	# Start scheduler
	scheduler_th = threading.Thread(target=scheduler.scheduler_thread)
	scheduler_th.start()

	# Process commands
	daemon_loop(daemon)

	socket_th.join()
	scheduler_th.join()

def check_and_logger_debug_if_already_running():
	"""
	Checks if the daemon is already running
	"""
	try:
		if (os.path.exists("/tmp/taskmaster.pid")):
			daemon.logger.info(f"Taskmaster daemon already running exiting...")
			return True
	except:
		return False

#from daemonize import Daemonize

if __name__ == "__main__":
	if not check_and_logger_debug_if_already_running():
		try :
			#d = Daemonize(app="supervisord", action=daemon_entry, pid="/tmp/taskmaster.pid", foreground=True)
			#d.start()
			daemon_entry()
		except Exception as e:
			daemon.logger.info("Daemon entry error: ", e)
			pass
		finally : 
			os.unlink("/tmp/taskmaster.pid")
			#d.exit()
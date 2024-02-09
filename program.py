
from collections import defaultdict
import os
import subprocess
import signal
from threading import Thread
from config import config_g
import functools

pids = defaultdict(list)

def worker(name, config):
	global pids
	"""
	This is the worker function that will run a process
	"""
	if "umask" in config:
		os.umask(int(config["umask"], 8))

	with open(config["stdout"], "a") as stdout, open(config["stderr"], "a") as stderr:
		for _ in range(config["startretries"]):
			# Run process
			try:
				process = subprocess.Popen(
					config["cmd"].split(" "),
					cwd = config["workingdir"],
					stdin = subprocess.DEVNULL,
					stdout=stdout,
					stderr=stderr,
					env = config["env"])
				pids[name].append(process.pid)
				break
			except:
				continue

def exec_program(program: str):
	global config_g

	expected_fields = set(["cmd", "numprocs", "autostart", "autorestart", "exitcodes", "startretries", "starttime", "stoptime", "stdout", "stderr", "workingdir"])

	config = config_g[program]
	if not config:
		raise ValueError(f"Program '{program}' has no configuration")

	if "env" not in config:
		config["env"] = {}

	if missing_fields := expected_fields - set(config):
			raise ValueError(f"Missing fields ({', '.join(missing_fields)}) in program '{program}'")

	ths = []

	for i in range(config["numprocs"]):
		print(f"Starting process {i} for program '{program}'")
		th = Thread(target=worker, args=(program, config))
		th.start()
		ths.append(th)

	for t in ths:
		t.join()

def stop_program(program: str):
	global pids
	global config_g

	config = config_g[program]
	if program not in pids:
		raise ValueError(f"Program '{program}' is not running")

	for pid in pids[program]:
		getit = getattr(signal, f"SIG{config['stopsignal']}")
		os.kill(pid, getit)
		check = os.waitpid(pid, os.WNOHANG)
		force_kill_pid(pid)



	# Remove from pid list
	del pids[program]


def force_kill_pid(pid):
	os.kill(pid, signal.SIGKILL)

# We received a sigchild, get terminated child and restart it if needed
def delete_pid(signum, frame):
	global pids
	global config_g

	pid, exit_code = os.waitpid(-1, os.WNOHANG)
	to_restart = ""
	for program in pids:
		if pid in pids[program]:
			to_restart = program
			break
	pids[program].remove(pid)
	if to_restart:
		if exit_code not in config_g[program]["exitcodes"] and config_g[program]["autorestart"] == "unexpected":
			worker(program, config_g[program])
		elif config_g[program]["autorestart"] == True:
			worker(program, config_g[program])

	# Create a copy of the dictionary to avoid "dictionary changed size during iteration" error
	"""
	pids_copy = pids.copy()

	for program in pids_copy:
		for pid in pids_copy[program]:
			try:
				signal.signal(signal.SIGALRM, functools.partial(force_kill_pid, pid))
				signal.alarm(config_g[program]["stoptime"])
				_, status = os.waitpid(pid, 0)
				pids[program].remove(pid)
			except OSError:
				pass
			finally:
				signal.alarm(0)
	"""
def run_all_programs():
	global config_g

	for program in config_g:
		if config_g[program]["autostart"]:
			exec_program(program)

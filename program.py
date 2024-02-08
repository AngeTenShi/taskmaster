
from collections import defaultdict
import os
import subprocess
import signal
from threading import Thread
from config import config_g

pids = defaultdict(list)

def worker(name):
	global pids
	global config_g
	"""
	This is the worker function that will run a process
	"""
	if "umask" in config_g:
		os.umask(int(config_g["umask"], 8))

	with open(config_g["stdout"], "a") as stdout, open(config_g["stderr"], "a") as stderr:
		for _ in range(config_g["startretries"]):
			# Run process
			try:
				process = subprocess.Popen(
					config_g["cmd"].split(" "),
					cwd = config_g["workingdir"],
					stdin = subprocess.DEVNULL,
					stdout=stdout,
					stderr=stderr,
					env = config_g["env"])
				pids[name].append(process.pid)
				break
			except:
				continue

def exec_program(program: str):
	expected_fields = set(["cmd", "numprocs", "autostart", "autorestart", "exitcodes", "startretries", "starttime", "stoptime", "stdout", "stderr", "workingdir"])
	global config_g

	config = config_g[program]
	if not config_g:
		raise ValueError(f"Program '{program}' has no configuration")

	if "env" not in config:
		config["env"] = {}

	if missing_fields := expected_fields - set(config[program]):
			raise ValueError(f"Missing fields ({', '.join(missing_fields)}) in program '{program}'")

	ths = []

	for i in range(config_g["numprocs"]):
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
		pids[program].remove(pid)

	for pid in pids[program]:
		check = os.waitpid(pid, os.WNOHANG)
		if check is None:
			os.kill(pid, signal.SIGKILL)

	# Remove from pid list
	del pids[program]


def delete_pid(signum, frame):
	global pids
	global config_g

	for program in pids:
		for pid in pids[program]:
			check = os.waitpid(pid, os.WNOHANG)
			if check is not None:
				pids[program].remove(pid)

def run_all_programs():
	global config_g
	print(config_g)
	for program in config_g:
		if config_g[program]["autostart"]:
			exec_program(program)

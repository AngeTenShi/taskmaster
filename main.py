import json
import argparse
import os
import subprocess

def worker(config: dict):
	"""
	This is the worker process that will be forked by the taskmaster
	"""
	os.umask(int(config["umask"], 8))
	with open(config["stdout"], "a") as stdout, open(config["stderr"], "a") as stderr:
		while True:
			process = subprocess.Popen(
				config["cmd"].split(" "),
				cwd = config["workingdir"],
				stdin = subprocess.DEVNULL,
				stdout=stdout,
				stderr=stderr,
				env = config["env"])

			#check process exit code
			process.wait()
			exitcode = process.returncode
			if config["autorestart"] == False or (config["autorestart"] == "unexpected" and exitcode in config["exitcodes"]):
				break


def exec_program(program: str, config : dict):
	expected_fields = set(["cmd", "numprocs", "autostart", "autorestart", "exitcodes", "startretries", "starttime", "stoptime", "stdout", "stderr", "workingdir"])

	if not config:
		raise ValueError(f"Program '{program}' has no configuration")

	if "env" not in config:
		config["env"] = {}

	if "umask" not in config:
		config["umask"] = "022"

	if missing_fields := expected_fields - set(config):
			raise ValueError(f"Missing fields ({', '.join(missing_fields)}) in program '{program}'")

	for i in range(config["numprocs"]):
		print(f"Starting process {i} for program '{program}'")
		env = os.environ.copy()
		env.update(config["env"])
		fils = os.fork()
		if fils == 0:
			worker(config)
			exit(0)
		else:
			print(f"Process {i} started for program '{program}'")

def taskmaster_main(config_path: str):
	"""
	This is the entrypoint of the taskmaster
	"""

	with open(config_path, 'r') as file:
		cfg = json.load(file)

	programs = cfg["programs"]

	for program in programs:
		if programs[program]["autostart"]:
			exec_program(program, programs[program])
	return 0


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument('-c', '--configfile', type=str, default='./config.json', help='Path to the configuration file')
	args = parser.parse_args()
	#try:
	taskmaster_main(args.configfile)
	#except Exception as e:
	#	print(f"Task master exited unexpectedly: {e}")


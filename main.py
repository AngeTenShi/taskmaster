# System
import json
import argparse
import signal

# Local imports
from shell import shell
import config
import program

def taskmaster_main():

	"""
	This is the entrypoint of the taskmaster
	"""
	# Setup signals
	signal.signal(signal.SIGCHLD, program.delete_pid)

	# Run all programs
	program.run_all_programs()

	# Give a shell
	return shell()


if __name__ == "__main__":
	#try:
 	# Parse arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('-c', '--configfile',required=True, type=str, help='Path to the configuration file')
	args = parser.parse_args()
	config.get_config(args.configfile)
	exit(taskmaster_main())
	#except Exception as e:
	#	print(f"Task master exited unexpectedly: {e}")


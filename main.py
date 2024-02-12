# System
import argparse
import threading
#import json
#import signal

# Local imports
#from shell import shell
#import config
#import program
from common import *

def taskmaster_main(configfile: str):
	"""
	This is the entrypoint of the taskmaster
	"""
	# Setup signals
	# signal.signal(signal.SIGCHLD, program.delete_pid)

	# Run all programs
	# program.run_all_programs()

	global q

	# Send configuration file over to the daemon
	with open(configfile, 'r') as file:
		q.put_nowait(Command(CommandType.RELOAD_CONFIG, [file.readlines()]))

	# This becomes the shell, in charge of sending commands to the daemon via the command queue
	while True:
		pass

	return 0


if __name__ == "__main__":
	#try:
 	# Parse arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('-c', '--configfile',required=True, type=str, help='Path to the configuration file')
	args = parser.parse_args()
	
	exit(taskmaster_main(args.configfile))
	#except Exception as e:
	#	print(f"Task master exited unexpectedly: {e}")


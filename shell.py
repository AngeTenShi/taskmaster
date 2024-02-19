
# Local imports
# import program
# from program import pids
# from config import config_g


# def stop_all():
# 	global pids

# 	cpy = pids.copy()
# 	for p in cpy:
# 		program.stop_program(p)

# def run_all():
# 	program.run_all_programs(config_g)

import socket
from common import Command, CommandType, List
import pickle

def get_command_type(command: str, args: List) -> Command:
	if command == "status":
		return Command(CommandType.STATUS, args)
	elif command == "reload":
		return Command(CommandType.RELOAD_CONFIG, args)
	elif command == "stop":
		return Command(CommandType.INTERNAL_STOP_PROC, args)
	elif command == "start":
		return Command(CommandType.INTERNAL_START_PROC, args)
	elif command == "restart":
		return Command(CommandType.INTERNAL_RESTART_PROC, args)
	else:
		return Command(CommandType.UNKNOWN, command)

def send_command(args, command_name: str):
	s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	try:
		s.connect("/tmp/daemon.sock")
		command = get_command_type(command_name, args)
		data = pickle.dumps(command)
		size = len(data).to_bytes(4, "little")
		s.send(size + data)
		s.close()
	except ConnectionRefusedError:
		print("Daemon is not running")
		return 1
	except Exception as e:
		s.close()
		return 1

def shell():
	while True:
		try:
			argv = input(">> ").split(" ")

			if len(argv) == 1 and len(argv[0]):
				if argv[0] == "status":
					pass
				elif argv[0] =="exit":
					send_command("all", "stop")
					return 0
				elif argv[0] == "help":
					print("lis le code")
					pass
				else:
					print(f"Unknown command {argv[0]}, does it have enough arguments?")

			elif len(argv) == 2:
				if argv[0] == "start":
					send_command(argv[1], "start")
				elif argv[0] == "stop":
					send_command(argv[1], "stop")
				elif argv[0] == "restart":
					send_command(argv[1], "restart")
				elif argv[0] == "reload":
					try:
						send_command([open(argv[1], 'r').read()], "reload")
					except:
						print(f"Could not open config {argv[1]}")
				else:
					print(f"Unknown command {argv[0]}, does it have the right number of arguments?")

			elif len(argv[0]):
				print(f"Unknown command ({' '.join(argv)}")
		except EOFError:
			return 0

if __name__ == "__main__":
	shell()

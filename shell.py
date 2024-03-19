import socket
import threading
import readline # enhances input() features
import pickle
import os
import signal


from common import CommandRequest, CommandResponse, CommandType

class DaemonConnection():
	class TimeoutError(Exception): ...

	def __init__(self):
		self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		self.sock.connect("/tmp/taskmaster.sock")
		self.last_id = 0
	
	def __del__(self):
		self.sock.close()

	def send_command(self, type, args):
		self.last_id += 1
		try:
			data = pickle.dumps(CommandRequest(type, args, self.last_id))
			size = len(data).to_bytes(4, "little")
			self.sock.send(size + data)

			return True, "success"
		except:
			return False, "unknown"
	
	def recv_answer(self):
		try:
			# make an alarm
			signal.alarm(15)
			sz = int.from_bytes(self.sock.recv(4, socket.MSG_WAITALL), 'little')
			data = self.sock.recv(sz, socket.MSG_WAITALL)
			signal.alarm(0)

			response : CommandResponse = pickle.loads(data)

			return response.id == self.last_id, response.response
		except pickle.UnpicklingError:
			return False, None
		pass
	

class ShouldExit(Exception): ...

def get_text_command():
	no_args = lambda args: len(args) == 0
	one_arg = lambda args: len(args) == 1
	at_least_one_arg = lambda args: len(args) > 0

	def exit():
		raise ShouldExit

	def help():
		print("Shell commands")
		print("\texit: exits the shell")
		print("\thelp: displays this")
		print("")
		print("Daemon commands")
		print("\tstatus <program1> <program2> ...: displays status of programs")
		print("\tstart <program1> <program2> ...: start programs")
		print("\tstop <program1> <program2> ...: stop programs")
		print("\trestart <program1> <program2> ...: restart programs")
		print("\treload <config>: loads a config, killing all old config programs, starting the ones with `autostart`")

	commands = {
		# Shell commands
		"exit": (None, no_args, exit),
		"help": (None, no_args, help),

		# Daemon commands
		"status": (CommandType.STATUS, no_args, None),

		"start": (CommandType.START_PROGRAM, at_least_one_arg, None),
		"stop": (CommandType.STOP_PROGRAM, at_least_one_arg, None),
		"restart": (CommandType.RESTART_PROGRAM, at_least_one_arg, None),

		"reload": (CommandType.RELOAD_CONFIG, one_arg, lambda args: [open(args[0], 'r').read()])
	}

	argv = input(">> ").split(" ")
	if len(argv) == 0 or not argv[0] in commands:
		print("Unknown command, type 'help' for a list of available commands.")
		return None, None

	daemon_cmd_type, validator, data = commands[argv[0]]
	if not validator(argv[1:]):
		print("Invalid number of arguments for command, type 'help' for usage.")
		return None, None

	if not daemon_cmd_type:
		data()
		return None, None

	args = argv[1:]
	if data:
		args = data(args)
	
	return daemon_cmd_type, args



def shell():
	def sa_handler(_, __):
		raise DaemonConnection.TimeoutError
	signal.signal(signal.SIGALRM, sa_handler)

	try:
		conn = DaemonConnection()
	except Exception as e:
		print("Could not connect to socket, is the daemon running ?")
		return 1

	while True:
		try:
			t, args = get_text_command()
			if not t:
				continue
			result, message = conn.send_command(t, args)
			if not result:
				print(f"`{message}` error while sending command, please retry...")
				continue
			result, data = conn.recv_answer()
			if not result:
				continue
			print(data)
		except DaemonConnection.TimeoutError:
			print("Shell did not receive a response in time, please check the daemon status and retry.")
			return 1
		except (EOFError, KeyboardInterrupt, ShouldExit):
			print("exiting...")
			return
		except:
			print("unhandled exception, please retry.")
			raise 
	
if __name__ == "__main__":
	shell()

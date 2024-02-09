
# Local imports
import program
from program import pids
from config import config_g


def stop_all():
	global pids

	cpy = pids.copy()
	for p in cpy:
		program.stop_program(p)

def run_all():
	program.run_all_programs(config_g)


def shell():
	global pids

	while True:
		try:
			argv = input(">> ").split(" ")

			if len(argv) == 1 and len(argv[0]):
				if argv[0] == "reload":
					stop_all()
					run_all()
				elif argv[0] == "status":
					pass
				elif argv[0] =="exit":
					stop_all()
					return 0
				elif argv[0] == "help":
					print("lis le code")
					pass
				else:
					print(f"Unknown command {argv[0]}, does it have enough arguments?")

			elif len(argv) == 2:
				if argv[0] == "start":
					pass
				elif argv[0] == "stop":
					p = argv[1]
					program.stop_program(p)
				elif argv[0] == "restart":
					p = argv[1]
					program.stop_program(p)
					program.worker(p)
				else:
					print(f"Unknown command {argv[0]}, does it have the right number of arguments?")

			elif len(argv[0]):
				print(f"Unknown command ({' '.join(argv)}")
		except EOFError:
			return 0

from enum import Enum
from typing import Dict, List
from queue import Queue
import logging

class Status(Enum):
	STOPPED = 1
	STARTING = 2
	RUNNING = 3
	BACKOFF = 4
	STOPPING = 5
	EXITED = 6
	FATAL = 7
	UNKNOWN = 8

class Daemon:
	def __init__(self):
		self.command_queue = Queue()
		self.output_queue = Queue()
		self.config = None
		self.programs : Dict[str, List[Program]] = {}
		self.logger = None
class Program:
	def __init__(self, name : str, config):
		self.name = name
		self.config = config
		self.pid = None
		self.exit_code = None
		self.start_retries = 0
		self.status = Status.STOPPED
		self.exit_timer = None
		self.start_timer = None
		self.process = None
		self.stack = None

	def should_auto_restart(self, exitcode):
		return self.config["autorestart"] == True or (self.config["autorestart"] == "unexpected" and exitcode not in self.config["exitcodes"])

	def set_running(self):
		self.status = Status.RUNNING

	def set_pid(self, pid):
		self.pid = pid

	def set_process(self, process):
		self.process = process

	def clear(self):
		self.pid = None
		self.exit_code = None
		self.process = None

		#self.start_retries = 0
		self.status = Status.STOPPED
		if self.exit_timer:
			self.exit_timer.cancel()
		self.exit_timer = None
		if self.start_timer:
			self.start_timer.cancel()
		# self.start_timer = None

daemon = Daemon()
logger = logging.getLogger('my_daemon')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('daemon.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.info('Daemon started')
daemon.logger = logger
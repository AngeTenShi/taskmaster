from .decorators import no_sigchld

import threading
import time
import queue

class ScheduledEvent:
	def __init__(self, ts, func):
		self.ts = ts
		self.func = func
		self.defused = False

	def __lt__(self, other):
		return self.ts < other.ts

	def timestamp(self):
		return self.ts

	def cancel(self):
		self.defused = True

	def exec(self):
		if not self.defused:
			self.func()

has_new_event = threading.Event()
new_events = []
interrupted = False

def schedule_event(s, func):
	global new_events
	global has_new_event

	e = ScheduledEvent(time.time() + s, func)
	has_new_event.set()
	new_events += [e]
	return e

def interrupt_scheduler():
	global interrupted
	global has_new_event

	interrupted = True
	# hack to interrupt the sleep, should not be used apart from tests
	has_new_event.set()

def clear():
	global new_events
	global has_new_event
	global interrupted

	new_events = []
	has_new_event.clear()
	interrupted = False
	
@no_sigchld
def scheduler_thread():
	global new_events
	global has_new_event
	global interrupted

	evs = queue.PriorityQueue()	

	# Wait for every event to happen and perform its logic, off by miliseconds errors are OK here.
	while not interrupted:
		most_recent = None

		# Compute the time for next event
		if not evs.empty():
			most_recent = evs.queue[0].timestamp() - time.time()

		# Wait for next event or new event
		try:
			has_timeout = not has_new_event.wait(most_recent)
		except ValueError:
			# If we tried to sleep negative time, it means we have something to do instantly, which means we can apply the `timeout` logic
			has_timeout = True

		# Execute the most recent event
		if has_timeout:
			e = evs.get_nowait()
			e.exec()
			evs.task_done()
		# New event, add it to the queue
		else:
			for e in new_events:
				evs.put_nowait(e)
			has_new_event.clear()
			new_events = []
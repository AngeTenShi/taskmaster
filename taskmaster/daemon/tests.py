from . import scheduler

import unittest
import threading
import random
import time

from datetime import timedelta


class SchedulerTest(unittest.TestCase):
	ACCEPTABLE_DELAY = timedelta(milliseconds=10).total_seconds()
	SAMPLES_COUNT = 50

	def setUp(self):
		self.scheduler_th = threading.Thread(target=scheduler.scheduler_thread)
		self.scheduler_th.start()

	def test_no_delay(self):
		# Test that we can schedule an event that should be executed 'almost' immediately
		res = threading.Event()

		scheduler.schedule_event(0.0, lambda: res.set())
		hasBeenSet = res.wait(0.0 + self.ACCEPTABLE_DELAY)

		self.assertTrue(hasBeenSet, "Event was not executed in time (0s)")
	
	def test_random_delay(self):
		for _ in range(5):
			# Test that we can schedule an event that should be executed 'almost' immediately
			res = threading.Event()
			delay = random.random()

			scheduler.schedule_event(delay, lambda: res.set())
			hasBeenSet = res.wait(delay + self.ACCEPTABLE_DELAY)

			self.assertTrue(hasBeenSet, f"Event was not executed in time ({delay}s)")
		
	def test_random_delay_simultaneous(self):
		# takes at most 5s to execute
		samples = [(random.random() * 5, threading.Event()) for _ in range(self.SAMPLES_COUNT)]

		# schedule them all, (random order is wanted, there needs to be priority)
		for delay, ev in samples:
			scheduler.schedule_event(delay, lambda: ev.set())

		starting_time = time.time()
		elapsed_time = 0
		
		# wait for them all to finish, in order
		samples.sort(key=lambda x: x[0])
		for delay, ev in samples:
			d = delay + self.ACCEPTABLE_DELAY - elapsed_time

			hasBeenSet = ev.wait(d)
			self.assertTrue(hasBeenSet, f"Event was not executed in time ({delay}s)")

			elapsed_time = time.time() - starting_time

	def test_random_delay_simultaneous_cancel(self):
		# takes at most 5s to execute, acceptable delay so that we have time to cancel it
		samples = [(random.uniform(self.ACCEPTABLE_DELAY, 1.0) * 5, threading.Event()) for _ in range(self.SAMPLES_COUNT)]

		# schedule them all, (random order is wanted, there needs to be priority in the scheduler)
		cancelled = []
		for delay, ev in samples:
			should_cancel = random.choice([True, False])
			e = scheduler.schedule_event(delay, lambda: ev.set())
			if should_cancel:
				e.cancel()
				cancelled.append((delay, ev))

		starting_time = time.time()
		elapsed_time = 0
		
		# wait for them all to finish, in order
		samples.sort(key=lambda x: x[0])
		for delay, ev in samples:
			d = delay + self.ACCEPTABLE_DELAY - elapsed_time

			# in case we already passed the delay, check if it was executed instantly
			hasBeenSet = ev.wait(d)
			if not (delay, ev) in cancelled:
				self.assertTrue(hasBeenSet, f"Event was not executed in time ({delay}s)")
			else:
				self.assertFalse(hasBeenSet, f"Event was executed when it should have been cancelled ({delay}s)")
		
			elapsed_time = time.time() - starting_time


	def tearDown(self):
		scheduler.interrupt_scheduler()
		self.scheduler_th.join()
		self.scheduler_th = None
		scheduler.clear()
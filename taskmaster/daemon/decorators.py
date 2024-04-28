import signal
import os

def block_signals(siglist):
	def decorator(f):
		def wrapper(*args, **kwargs):
			signal.pthread_sigmask(signal.SIG_BLOCK, siglist)
			res = f(*args, **kwargs)
			signal.pthread_sigmask(signal.SIG_UNBLOCK, siglist)
			return res
		return wrapper
	return decorator

def no_sigchld(f):
	def wrapper(*args, **kwargs):
		signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGCHLD])
		res = f(*args, **kwargs)
		return res
	return wrapper
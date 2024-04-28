import signal
import os

def block_signals(siglist):
	def decorator(f):
		def wrapper(*args, **kwargs):
			signal.pthread_sigmask(signal.SIG_BLOCK, siglist)
			os.write(1, bytes("signals blocked\n", "utf-8"))
			#signal.signal(signal.SIGCHLD, signal.SIG_IGN)
			res = f(*args, **kwargs)
			#signal.signal(signal.SIGCHLD, lambda s,f: handle_sigchld(daemon))
			os.write(1, bytes("signals unblocked\n", "utf-8"))
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
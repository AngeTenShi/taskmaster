from dataclasses import dataclass
from enum import Enum
from typing import  List

from queue import Queue

"""
Interface for talking over to the daemon
"""
class CommandType(Enum):
	RELOAD_CONFIG = 0,
	START_PROGRAM = 1,
	STOP_PROGRAM = 2,
	RESTART_PROGRAM = 3,
	STATUS = 4,
	ABORT = 5,
	INTERNAL_START_PROC = 6,
	INTERNAL_STOP_PROC = 7
	INTERNAL_RESTART_PROC = 8

@dataclass
class Command:
	id: CommandType
	args: List

type CommandQueue = Queue[Command]

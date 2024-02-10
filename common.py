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
	INTERNAL_START_PROC = 6

type CommandArgs = List[str]

@dataclass
class Command:
	id: CommandType
	args: CommandArgs

type CommandQueue = Queue[Command]

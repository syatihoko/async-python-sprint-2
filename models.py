from dataclasses import dataclass
from typing import List, TypeVar


JobState = TypeVar('JobState')


@dataclass
class JobState:
    name: str
    time_start: float
    time_spend: float
    time_limit: float
    restart_allowed: int
    restart_happened: int
    dependencies: List[JobState]
    is_running: bool = False

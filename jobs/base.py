from uuid import uuid4
from datetime import datetime
from typing import Optional, List


from logger.logger import log


class JobBase:
    def __init__(self,
                 uniq_id: Optional[str] = None,
                 dependencies: Optional[List[str]] = None,
                 start_at: Optional[datetime] = None,
                 max_working_time: int = -1,
                 fails_counter: int = 0,
                 tries: int = -1):
        """
        Base Job class serves as an expected interface and gives some
        common mechanism for other jobs, so they fit Scheduler expectations
        :param uniq_id: uniq id among all tasks
        :param dependencies: List of other tasks uniq_ids
        the current task depends on
        :param start_at: Time we want the task to start at
        :param max_working_time: Maximum time in seconds
        we give to the task to run
        :param tries: maximum number of times allowed for the task to fail
        """
        self.start_at = start_at
        self.started_at = None
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.time_spent = 0
        self.fails_counter = fails_counter
        self.uniq_id = uniq_id if uniq_id else uuid4().hex

    def init(self):
        self.started_at = datetime.now()

    def prepare_to_save(self):
        return self

    def restore_from_save(self):
        return self

    def stop(self):
        log.info('%s: finishing itself', self.__class__.__name__)

    def __iter__(self):
        return self

    def __next__(self):
        raise NotImplementedError

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.uniq_id}'

import os
from time import sleep

from jobs.base import JobBase
from logger.logger import log


class FileCreateJob(JobBase):

    def __init__(self, file_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._file_name = file_name

    def init(self):
        super().init()
        log.info('%s: Starting up file creation', self.__class__. __name__)
        return iter(self)

    def _is_file_exists(self) -> bool:
        log.debug('%s: Checking if file exists', self.__class__.__name__)
        if os.path.isfile(self._file_name):
            log.info('%s: file %s exists',
                     self.__class__.__name__, self._file_name)
            return True
        return False

    def __next__(self):
        yield
        if not self._is_file_exists():
            yield
            with open(self._file_name, 'w'):
                log.info('%s: Creating %s',
                         self.__class__.__name__, self._file_name)


class FileDeleteJob(JobBase):

    def __init__(self, file_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._file_name = file_name

    def init(self):
        super().init()
        log.info('%s: Starting up file deletion', self.__class__.__name__)

    def _is_file_exists(self) -> bool:
        log.debug('%s: Checking if file exists', self.__class__.__name__)
        if os.path.isfile(self._file_name):
            log.info('%s: file %s exists',
                     self.__class__.__name__, self._file_name)
            return True
        return False

    def __next__(self):
        yield
        if self._is_file_exists():
            yield
            os.remove(self._file_name)


class LongLastingTask(JobBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ticks = 0

    def init(self):
        super().init()
        log.info('%s: Starting up long lasting task', self.__class__.__name__)

    def __next__(self):
        yield
        while True:
            self._ticks += 1
            if self._ticks == 1000:
                log.info('%s: still running', self.__class__.__name__)
                self._ticks = 0
            sleep(0.001)
            yield

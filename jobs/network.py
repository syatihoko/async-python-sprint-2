from typing import List
from collections import deque

import requests

from jobs.base import JobBase
from logger.logger import log


class GetURLJob(JobBase):

    def __init__(self, urls: List[str], out_queue: deque, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._urls = urls
        self.queue = out_queue

    def run(self):
        log.info('%s: Starting up URL fetching process',
                 self.__class__.__name__)

    def __next__(self):
        yield
        for url in self._urls:
            try:
                with requests.get(url=url) as req:
                    self.queue.append(req.text)
            except requests.exceptions.RequestException as e:
                log.error('%s: Got exception while requesting %s. %s',
                          self.__class__.__name__, url, e, exc_info=True)
            yield
        log.info('%s: Finishing', self.__class__.__name__)
        self.queue.append(0)


class URLResponseAnalyser(JobBase):

    def __init__(self, in_queue: deque, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = in_queue

    def run(self):
        log.info('%s: Starting up URL analyser',
                 self.__class__.__name__)

    def __next__(self):
        yield
        while True:
            try:
                url_response = self.queue.popleft()
                if not url_response:
                    log.info('%s: all responses are processed',
                             self.__class__.__name__)
                    return
                log.info('%s: response length is %s',
                         self.__class__.__name__, len(url_response))
            except IndexError:
                log.debug('%s: nothing to analyse yet. Waiting',
                          self.__class__.__name__)
            yield

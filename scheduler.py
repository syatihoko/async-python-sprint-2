import json
import queue
import pickle
from queue import Queue
from typing import Dict, List
from itertools import cycle
from time import perf_counter
from datetime import datetime
from dataclasses import dataclass, fields, field

from jobs.base import JobBase
from logger.logger import log


@dataclass
class SchedulerState:
    running_tasks: List = field(default_factory=list)
    task_pool: List = field(default_factory=list)
    done_tasks: List = field(default_factory=list)
    failed_tasks: List = field(default_factory=list)
    token_pool_current_size: int = 0

    def to_dict(self):
        return {
            field.name: getattr(self, field.name) for field in fields(self)
        }


class Scheduler:
    def __init__(self, pool_size=10):
        self._token_pool = self._init_pool_queue(size=pool_size)
        self._task_pool = Queue()  # type: Queue[JobBase]
        self._running_tasks = {}  # type: Dict[JobBase]
        self._done_tasks = set()  # type: Set[str]
        self._failed_tasks = set()  # type: Set[str]

    @staticmethod
    def _init_pool_queue(size: int) -> Queue:
        pool = Queue(maxsize=size)  # type: Queue
        for _ in range(size):
            pool.put(1)
        return pool

    def schedule(self, task: JobBase):
        self._task_pool.put(task)

    def _is_task_run_out_of_time(self, task: JobBase) -> bool:
        """Checks if task has no time left for running"""
        if task.max_working_time != -1:
            if task.max_working_time <= task.time_spent:
                log.info(
                    '%s: task %s exceeded time limits. Stopping it',
                    self.__class__.__name__, task)
                return True
        return False

    def _is_task_run_out_of_fails(self, task: JobBase) -> bool:
        """If we exceeded failed limit number"""
        if task.tries != -1:
            if task.fails_counter >= task.tries:
                log.info('%s: too many fails for task %s',
                         self.__class__.__name__, task)
                return True
        return False

    def _clean_up_task(self, task: JobBase,
                       force: bool = False, success: bool = True):
        """
        Does all the work that needs to be done to clean up after
        task was done or failed
        :param task: task itself
        :param force: if we want to cancel the task
        :param success: marks if the task was done successfully or not
        """
        if force:
            task.stop()
        self._token_pool.put(1)
        del self._running_tasks[task]
        if success:
            self._done_tasks.add(task.uniq_id)
        else:
            self._failed_tasks.add(task.uniq_id)

    def _has_waiting_dependencies(self, task: JobBase) -> bool:
        """
        Checks if the task can be run and has all
        the dependency tasks done or has no dependency at all
        """
        if task.dependencies:
            if set(task.dependencies) - self._done_tasks:
                return True
        return False

    def _has_dependent_task_failed(self, task: JobBase) -> bool:
        """
        Checks if the task has any failed dependencies
        """
        if task.dependencies:
            if set(task.dependencies).intersection(self._failed_tasks):
                return True
        return False

    @staticmethod
    def _has_right_time_to_start(task: JobBase) -> bool:
        """
        If task has start_at field defined then function checks if
        time has come to run the function
        """
        if task.start_at:
            if datetime.now() >= task.start_at:
                return True
            return False
        return True

    def _place_task_back_to_queue(self, task):
        self._task_pool.put(task)
        self._token_pool.put(1)

    def _run_loop_once(self):

        # Running some kind of event loop
        for task in cycle(self._running_tasks):
            if any([self._is_task_run_out_of_fails(task),
                    self._is_task_run_out_of_time(task)]):
                self._clean_up_task(
                    task=task, force=True, success=False)
                break

            else:
                try:
                    start_time = perf_counter()
                    next(self._running_tasks[task])
                    task.time_spent \
                        += (perf_counter() - start_time)

                    # if we have spare tokens and waiting
                    # to run tasks - we go there and try to get one
                    if not self._task_pool.empty() \
                            and not self._token_pool.empty():
                        break

                except StopIteration:
                    log.info('%s: task %s successfully done',
                             self.__class__.__name__, task)
                    self._clean_up_task(task)
                    break

                except Exception as e:
                    log.exception(
                        '%s: Got exception '
                        'while running task %s: %s',
                        self.__class__.__name__, task, e)
                    if task.tries == -1:
                        self._clean_up_task(
                            task=task, force=True, success=False)
                        break
                    else:
                        task.fails_counter += 1
                        self._running_tasks[task] = next(task)

    def _add_task_to_loop(self, task: JobBase):
        # If we have failed dependencies,
        # there is no point on running this task,
        # so we need to cancel it
        if self._has_dependent_task_failed(task=task):
            log.info('%s: Can not run task %s '
                     'as it has failed dependencies',
                     self.__class__.__name__, task)
            self._failed_tasks.add(task.uniq_id)
            self._token_pool.put(1)

        # if task has dependencies that are not done yet,
        # then we place this task to the task pool again
        elif self._has_waiting_dependencies(task=task):
            log.info('%s: task %s has a waiting dependency',
                     self.__class__.__name__, task)
            self._place_task_back_to_queue(task=task)

        elif not self._has_right_time_to_start(task=task):
            log.info('%s: task %s time has not come yet',
                     self.__class__.__name__, task)
            self._place_task_back_to_queue(task=task)

        else:
            task.init()
            self._running_tasks[task] = next(task)

    def run(self):
        """Starts the scheduler loop"""
        try:
            while True:
                token = self._token_pool.get(block=False)
                if token and not self._task_pool.empty():
                    task = self._task_pool.get(block=False)
                    if not task:
                        log.info('%s: No more tasks left to process. Exiting',
                                 self.__class__.__name__)
                        return

                    self._add_task_to_loop(task=task)

                if self._running_tasks:
                    self._run_loop_once()

                elif not self._task_pool.empty():
                    continue

                else:
                    log.info('%s: No more Jobs to run. Exiting',
                             self.__class__.__name__)
                    return
        except KeyboardInterrupt:
            log.info('%s: Finishing and saving current state',
                     self.__class__.__name__)
            self.stop()

    def restart(self):
        pass

    def get_current_state(self) -> Dict:
        current_state_struct = SchedulerState()
        for task in self._running_tasks:
            current_state_struct.running_tasks.append(
                pickle.dumps(task.prepare_to_save()).hex())

        for task_uniq_id in self._done_tasks:
            current_state_struct.done_tasks.append(task_uniq_id)

        for task_uniq_id in self._failed_tasks:
            current_state_struct.failed_tasks.append(task_uniq_id)

        try:
            while task := self._task_pool.get(block=False):
                current_state_struct.task_pool.append(
                    pickle.dumps(task.prepare_to_save()).hex())
        except queue.Empty:
            pass

        current_state_struct.token_pool_current_size \
            = self._token_pool.qsize()
        return current_state_struct.to_dict()

    def load(self, file: str):
        with open(file, 'r') as _f:
            config = json.load(_f)

        # filling up running tasks structure
        for task_config in config['running_tasks']:
            task = pickle.loads(bytes.fromhex(task_config))  # type: JobBase
            task.restore_from_save()
            log.info('%s: loaded task %s',
                     self.__class__.__name__, task.__class__.__name__)
            self._running_tasks[task] = next(task)

        # filling up task queue
        for task_config in config['task_pool']:
            task_n = pickle.loads(bytes.fromhex(task_config))  # type: JobBase
            task_n.restore_from_save()
            self._task_pool.put(task_n)

        # filling up failed and done tasks
        self._failed_tasks = set(config['failed_tasks'])
        self._done_tasks = set(config['done_tasks'])

        tokens_to_drop = \
            self._token_pool.maxsize - config['token_pool_current_size']

        for _ in range(tokens_to_drop):
            self._token_pool.get_nowait()

        log.info('%s: done loading saved state', self.__class__.__name__)

    def _write_state_to_json(self, state: Dict):
        file_name = f'{datetime.now().isoformat()}.json'
        log.info('%s: writing state to json file %s',
                 self.__class__.__name__, file_name)
        with open(file_name, 'w') as _f:
            json.dump(state, _f)

    def stop(self):
        """Stops all the running tasks and saves the current state"""
        # current_state = self.get_current_state()
        current_state = self.get_current_state()
        self._write_state_to_json(state=current_state)

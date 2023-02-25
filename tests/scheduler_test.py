from time import sleep
from datetime import datetime, timedelta
from typing import Callable, Optional

from scheduler import Scheduler
from jobs.base import JobBase


class TestJob(JobBase):
    __test__ = False

    def __init__(self, next_func: Callable, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_called = False
        self._next_callable_func = next_func
        self.started_at = None  # type: Optional[datetime]

    def init(self):
        self.started_at = datetime.now()

    def stop(self):
        self.stop_called = True

    def __next__(self):
        yield from self._next_callable_func()


def test_failed_job_detected():
    """Detects if job failed more than expected and drops it"""
    def next_func():
        yield
        raise ValueError

    max_allowed_fails = 2
    job = TestJob(tries=max_allowed_fails, next_func=next_func)
    scheduler = Scheduler()
    scheduler.schedule(task=job)
    scheduler.run()
    assert job.fails_counter == max_allowed_fails
    assert job.stop_called


def test_run_out_of_time_job_detected():
    """Detects if job failed more than expected and drops it"""
    def next_func():
        yield
        while True:
            sleep(0.5)
            yield

    max_time_allowed = 1
    job = TestJob(max_working_time=max_time_allowed, next_func=next_func)
    scheduler = Scheduler()
    scheduler.schedule(task=job)
    scheduler.run()
    assert job.time_spent >= max_time_allowed
    assert job.stop_called


def test_scheduler_exit_if_has_no_tasks():
    """If scheduler has no task to run it should stop"""
    scheduler = Scheduler()
    scheduler.run()
    assert len(scheduler._done_tasks) == 0


def test_job_not_run_if_dependent_task_failed():
    """
    If a task has a failed to run dependency, then it also no run
    """
    def failed_func():
        yield
        raise ValueError

    def do_nothing_func():
        yield

    failed_job = TestJob(next_func=failed_func)
    normal_job = TestJob(next_func=do_nothing_func,
                         dependencies=[failed_job.uniq_id])

    scheduler = Scheduler()
    scheduler.schedule(failed_job)
    scheduler.schedule(normal_job)
    scheduler.run()

    assert len(scheduler._failed_tasks) == 2
    assert len(scheduler._done_tasks) == 0


def test_job_started_at_time_specified():
    """
    Testing that task starts at specific time (not earlier)
    """
    def do_nothing_func():
        yield

    expecting_start_at = datetime.now() + timedelta(seconds=1)
    normal_job = TestJob(next_func=do_nothing_func,
                         start_at=expecting_start_at)
    scheduler = Scheduler()
    scheduler.schedule(normal_job)
    scheduler.run()
    assert normal_job.started_at >= expecting_start_at

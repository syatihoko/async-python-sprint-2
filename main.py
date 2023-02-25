import logging
import argparse
from collections import deque

from logger.logger import log
from scheduler import Scheduler
from jobs.filesystem import FileCreateJob, FileDeleteJob, LongLastingTask
from jobs.network import GetURLJob, URLResponseAnalyser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', '-v',
                        help='Verbose output', action='store_true')
    parser.add_argument('--load', '-l',
                        type=str, required=False)

    arguments = parser.parse_args()

    if arguments.verbose:
        log.setLevel(logging.DEBUG)

    file_name = '/home/tave/PycharmProjects/async-python-sprint-2/test.txt'

    scheduler = Scheduler()
    if arguments.load:
        scheduler.load(file=arguments.load)

    else:

        # file operations
        create_file_task = FileCreateJob(file_name=file_name)
        delete_file_task = FileDeleteJob(
            file_name=file_name,
            dependencies=[create_file_task.uniq_id])

        long_running_task = LongLastingTask()

        job_queue = deque()
        url_fetching_task = GetURLJob(
            urls=[
                'https://ya.ru',
                'https://google.com',
                'https://bing.com'
            ],
            out_queue=job_queue)
        url_analyser_task = URLResponseAnalyser(in_queue=job_queue)

        scheduler.schedule(task=create_file_task)
        scheduler.schedule(task=delete_file_task)
        scheduler.schedule(task=long_running_task)
        scheduler.schedule(task=url_fetching_task)
        scheduler.schedule(task=url_analyser_task)

    scheduler.run()


if __name__ == '__main__':
    main()

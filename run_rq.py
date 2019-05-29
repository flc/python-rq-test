import argparse
import sys
import time
from collections import deque
import logging

from redis import Redis
from rq import Queue

from recycle.looping import grouper

from smiles import split_components


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger = logging.getLogger(__name__)




def process_job_result(result):
    for smiles in result:
        sys.stdout.write(smiles)
        sys.stdout.write('\n')


def check_jobs(ajobs):
    if not ajobs:
        return ajobs

    logger.debug('check jobs: %s', ajobs)
    to_remove = set()
    while True:
        is_job_finished = False
        for job in ajobs:
            if job.result is not None:
                logger.debug('job finished: %s', job)
                process_job_result(job.result)
                to_remove.add(job)
                is_job_finished = True
        if not is_job_finished:
            time.sleep(1)
        else:
            break
    return [job for job in ajobs if job not in to_remove]


def main(chunk_size=1000, concurrency=1, result_ttl=60):
    q = Queue(connection=Redis())

    ajobs = []

    for idx, smiles_list_chunk in enumerate(grouper(chunk_size, sys.stdin), 1):
        ajob = q.enqueue(
            split_components,
            result_ttl=60,
            args=(smiles_list_chunk,),
            description='split_components for chunk {}'.format(idx),
            )
        ajobs.append(ajob)
        if len(ajobs) >= concurrency:
            ajobs = check_jobs(ajobs)
            logger.debug('returned jobs: %s', ajobs)

    logger.debug('finish: %s', ajobs)
    while ajobs:
        ajobs = check_jobs(ajobs)
    logger.debug('finish: %s', ajobs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--chunk-size', type=int, default=1000,
        )
    parser.add_argument(
        '--concurrency', type=int, default=1,
        )
    parser.add_argument(
        '--log-level', type=str, default='DEBUG',
        )
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    logging.root.setLevel(log_level)

    main(
        chunk_size=args.chunk_size,
        concurrency=args.concurrency,
        )

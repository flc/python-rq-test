import argparse
import sys
import time
from collections import deque
import logging
from multiprocessing import Pool, TimeoutError
import os

from recycle.looping import grouper

from smiles import split_components


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


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


def main(chunk_size=1000, concurrency=1):
    with Pool(processes=concurrency) as pool:

        jobs = []
        for idx, smiles_list_chunk in enumerate(
            grouper(chunk_size, sys.stdin), 1
            ):
            job = pool.apply_async(split_components, (smiles_list_chunk,))
            jobs.append(job)
            logger.info('jobs: %s', jobs)
            # process_job_result(job.get())

        # for job in jobs:
        #     process_job_result(job.get())



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--chunk-size', type=int, default=1000,
        )
    parser.add_argument(
        '--concurrency', type=int, default=1,
        )
    args = parser.parse_args()

    main(
        chunk_size=args.chunk_size,
        concurrency=args.concurrency,
        )

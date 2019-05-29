import argparse
import sys
import os
import logging
import time
import multiprocessing
from collections import deque

from recycle.looping import grouper

from smiles import split_components


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


def process_job_result(result):
    for smiles in result:
        sys.stdout.write(smiles)
        sys.stdout.write('\n')


def main(chunk_size=1000, concurrency=1):
    with multiprocessing.Pool(processes=concurrency) as pool:

        jobs = []
        for idx, smiles_list_chunk in enumerate(
            grouper(chunk_size, sys.stdin), 1
            ):
            job = pool.apply_async(split_components, (smiles_list_chunk,))
            jobs.append(job)
            logger.debug('jobs: %s', jobs)
            # process_job_result(job.get())

        for job in jobs:
            process_job_result(job.get())



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--chunk-size', type=int, default=1000,
        )
    parser.add_argument(
        '--concurrency', type=int, default=multiprocessing.cpu_count(),
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

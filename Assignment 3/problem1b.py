#!/usr/bin/env python
import multiprocessing as mp# See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
import time
from math import pi
import matplotlib.pyplot as plt

# Each worker runs this function
def sample_pi(result_queue, seed, batch_size):
    # Using a static start seed, trying to reduce variability between runs.
    random.seed(seed)
    while (True):
        s = 0
        b = random.randint(int(batch_size / 2), int(batch_size * 2))
        for i in range (b):    
            x = random.random()
            y = random.random()
            if x**2 + y**2 <= 1.0:
                s += 1
        result_queue.put((s, b))    
    
# Gathers results from each worker and calculates accuracy
def estimator(result_queue, workers, acc_target):
    n_total = 0
    s_total = 0
    accuracy = 0
    total_queue_wait = 0
    while (accuracy < acc_target):
        start = time.time()
        successes, batch = result_queue.get()
        total_queue_wait += (time.time() - start)
        n_total += batch
        s_total = s_total + successes
        pi_est = (4.0*s_total)/n_total
        error = pi-pi_est
        accuracy = 1- abs(error)/pi
    return n_total, s_total, pi_est, error, accuracy, total_queue_wait

def compute_pi(args):

    # Starting time for measurement
    start = time.time()
    # Queue used by workers to send successes to the estimator method
    result_queue = mp.Queue()

    jobs = []
    # Number of sucesses each worker calculates before sending it to the result queue
    batch_size = 1000



    # A static (different) seed is used by each worker to reduce variability.
    for seed in range(args.workers):
        p = mp.Process(target=sample_pi, daemon = True, args=(result_queue,seed, batch_size))
        jobs.append(p)
        p.start()
    n_total, s_total, pi_est, error, accuracy, total_queue_wait = estimator(result_queue, args.workers, args.accuracy)

    # Terminating and then joining every working to avoid zombie processes
    for i in range(args.workers):
        jobs[i].terminate()

    for i in range(args.workers):
        jobs[i].join()

    result_queue.close()
    result_queue.join_thread()

    print(" Steps\tSuccess\tPi est.\tError\tAccuracy")
    print("%6d\t%7d\t%1.5f\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, error, accuracy))

    # Ending time for measurement
    measured_time = time.time() - start
    print("Total time\tQueue wait")
    print("%1.4f\t\t%1.4f" % (measured_time, total_queue_wait))

    return n_total / measured_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute Pi using Monte Carlo simulation.',
        epilog = 'Example: mp-pi-montecarlo-pool.py -s 100000 -w 4'
    )
    parser.add_argument('--workers', '-w',
                        default='1',
                        type = int,
                        help='Number of parallel processes')
    parser.add_argument('--accuracy', '-a',
                        default='0.9999999',
                        type = float,
                        help='Accuracy target for the Monte Carlo simulation')
    parser.add_argument('--speedup', '-s',
                        default = False,
                        action='store_true',
                        required=False,
                        help='Run automatic test for speedup graph or not.')
    args = parser.parse_args()
    compute_pi(args)

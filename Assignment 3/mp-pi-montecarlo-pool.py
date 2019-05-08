#!/usr/bin/env python
import multiprocessing as mp# See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
import queue
from math import pi

def sample_pi(result_queue):
    while (True):
        random.seed(10)
        s = 0
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            s += 1
        result_queue.put(s)
    

def estimator(result_queue, workers, acc_target):
    n_total = 0
    s_total = 0
    accuracy = 0
    while (accuracy < acc_target):
        successes = result_queue.get()
        n_total += 1
        s_total = s_total + successes
        pi_est = (4.0*s_total)/n_total
        error = pi-pi_est
        accuracy = 1- abs(error)/pi
    return n_total, s_total, pi_est, error, accuracy

def compute_pi(args):

    result_queue = mp.Queue()

    jobs = []

    for i in range(args.workers):
        p = mp.Process(target=sample_pi, args=(result_queue,))
        jobs.append(p)
        p.start()
    n_total, s_total, pi_est, error, accuracy = estimator(result_queue, args.workers, args.accuracy)

    for i in range(args.workers):
        jobs[i].terminate()
        print("Terminated job %6d" % i)   

    result_queue.close()
    print("Closed result queue")

    result_queue.join_thread()
    print("joined result queue")

    print(" Steps\tSuccess\tPi est.\tError\tAccuracy")
    print("%6d\t%7d\t%1.5f\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, error, accuracy))


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
                        default='0.999',
                        type = float,
                        help='Accuracy target for the Monte Carlo simulation')
    args = parser.parse_args()
    compute_pi(args)

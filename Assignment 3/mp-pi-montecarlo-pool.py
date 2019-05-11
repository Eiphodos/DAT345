#!/usr/bin/env python
import multiprocessing as mp# See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
import time
from math import pi
import matplotlib.pyplot as plt

# Each worker runs this method
def sample_pi(result_queue, seed, batch_size):
    # Using a static start seed, trying to reduce variability between runs.
    random.seed(seed)
    while (True):
        s = 0
        for i in range (batch_size):    
            x = random.random()
            y = random.random()
            if x**2 + y**2 <= 1.0:
                s += 1
        result_queue.put(s)    
    
def estimator(result_queue, workers, acc_target, batch_size):
    n_total = 0
    s_total = 0
    accuracy = 0
    while (accuracy < acc_target):
        successes = result_queue.get()
        n_total += batch_size
        s_total = s_total + successes
        pi_est = (4.0*s_total)/n_total
        error = pi-pi_est
        accuracy = 1- abs(error)/pi
    return n_total, s_total, pi_est, error, accuracy

def compute_pi(args):
    # Queue used by workers to send successes to the estimator method
    result_queue = mp.Queue()

    jobs = []
    # Number of sucesses each worker calculates before sending it to the result queue
    batch_size = 1000

    # Starting time for measurement
    start = time.time()

    # A static (different) seed is used by each worker to reduce variability.
    for seed in range(args.workers):
        p = mp.Process(target=sample_pi, args=(result_queue,seed, batch_size))
        jobs.append(p)
        p.start()
    n_total, s_total, pi_est, error, accuracy = estimator(result_queue, args.workers, args.accuracy, batch_size)

    # Ending time for measurement
    measured_time = time.time() - start

    # A more sophisticated way to do this would be to send a message to each worker
    # from the estimator once accuracy has been reached that tells them to stop working
    # and then running join for each worker.
    for i in range(args.workers):
        jobs[i].terminate()
        print("Terminated job %6d" % i)   

    result_queue.close()
    print("Closed result queue")

    result_queue.join_thread()
    print("joined result queue")

    print(" Steps\tSuccess\tPi est.\tError\tAccuracy")
    print("%6d\t%7d\t%1.5f\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, error, accuracy))
    return measured_time / n_total
    
# Calls compute_pi for k = 1, 2, 4, 8, 16 and 32
# Calculates speedup based the time it takes for k=1 to calculate 1 step
# Finally saves the result to a graph.
def print_speedup(args):

    fig, ax = plt.subplots()
    theory_list_x = []
    theory_list_y = []
    measure_list_x = []
    measure_list_y = []

    k = args.workers
    actual_speedup = k

    time_per_step_k_one = compute_pi(args)

    theory_list_x.append(k)
    theory_list_y.append(k)
    measure_list_x.append(k)
    measure_list_y.append(actual_speedup)

    k = k * 2
    args.workers = k

    while (k < 64):
        time_per_step_k = compute_pi(args)
        actual_speedup = time_per_step_k_one / time_per_step_k
        theory_list_x.append(k)
        theory_list_y.append(k)
        measure_list_x.append(k)
        measure_list_y.append(actual_speedup)
        k = k * 2
        args.workers = k
    ax.set(xlabel='k', ylabel='Speedup', title='Test done with accuracy goal 0.9999999')
    ax.grid()
    ax.plot(theory_list_x, theory_list_y, label='Theoretical speedup')
    ax.plot(measure_list_x, measure_list_y, label='Measured speedup')
    ax.legend(loc='upper left')
    plt.savefig('accuracy.png')

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
    if args.speedup is True:
        # Override any manual worker setting user might have set
        args.workers = 1
        print_speedup(args)
    else:
        compute_pi(args)

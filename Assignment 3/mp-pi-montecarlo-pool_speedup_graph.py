#!/usr/bin/env python
import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi
import time
import matplotlib.pyplot as plt

def sample_pi(n):
    """ Perform n steps of Monte Carlo simulation for estimating Pi/4.
        Returns the number of sucesses."""
    random.seed()
    s = 0
    for i in range(n):
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            s += 1
    return s


def compute_pi(args):
    fig, ax = plt.subplots()
    theory_list_x = []
    theory_list_y = []
    measure_list_x = []
    measure_list_y = []

    k = args.workers
    actual_speedup = k

    start = time.time()
    n = int(args.steps / k)
    
    p = multiprocessing.Pool(k)
    s = p.map(sample_pi, [n]*k)

    n_total = n*k
    s_total = sum(s)
    pi_est = (4.0*s_total)/n_total
    time_k_one = time.time() - start

    print(" Steps\tSuccess\tPi est.\tError")
    print("%6d\t%7d\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, pi-pi_est))
    print(" k\tTheoretical Speedup\tMeasured Speedup\tMeasured time")
    print("%6d\t\t%7d\t\t%1.5f\t\t\t%1.5f" % (k, k, actual_speedup,time_k_one))

    theory_list_x.append(k)
    theory_list_y.append(k)
    measure_list_x.append(k)
    measure_list_y.append(actual_speedup)
    p.close()
    k = k * 2
    while (k < 64):
        start = time.time()
        n = int(args.steps / k)
        
        p = multiprocessing.Pool(k)
        s = p.map(sample_pi, [n]*k)

        n_total = n*k
        s_total = sum(s)
        pi_est = (4.0*s_total)/n_total
        time_k = time.time() - start
        actual_speedup = time_k_one / time_k
        print(" Steps\tSuccess\tPi est.\tError")
        print("%6d\t%7d\t%1.5f\t%1.5f" % (n_total, s_total, pi_est, pi-pi_est))
        print(" k\tTheoretical Speedup\tMeasured Speedup\tMeasured time")
        print("%6d\t\t%7d\t\t%1.5f\t\t\t%1.5f" % (k, k, actual_speedup,time_k))

        theory_list_x.append(k)
        theory_list_y.append(k)
        measure_list_x.append(k)
        measure_list_y.append(actual_speedup)
        p.close()
        k = k * 2
    ax.set(xlabel='k', ylabel='Speedup', title='Test done with 10 000 000 steps')
    ax.grid()
    ax.plot(theory_list_x, theory_list_y, label='Theoretical speedup')
    ax.plot(measure_list_x, measure_list_y, label='Measured speedup')
    ax.legend(loc='upper left')
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute Pi using Monte Carlo simulation.',
        epilog = 'Example: mp-pi-montecarlo-pool.py -s 100000 -w 4'
    )
    parser.add_argument('--workers', '-w',
                        default='1',
                        type = int,
                        help='Number of parallel processes')
    parser.add_argument('--steps', '-s',
                        default='10000000',
                        type = int,
                        help='Number of steps in the Monte Carlo simulation')
    args = parser.parse_args()
    compute_pi(args)

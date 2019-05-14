#!/usr/bin/env python
import multiprocessing # See https://docs.python.org/3/library/multiprocessing.html
import argparse # See https://docs.python.org/3/library/argparse.html
import random
from math import pi
import time
import matplotlib.pyplot as plt

def print_speedup(args):

    fig, ax = plt.subplots()
    theory_list_x = [1, 2, 4, 8, 16, 32]
    theory_list_y = [1, 2, 4, 8, 16, 32]
    measure_list_x = [1, 2, 4, 8, 16, 32]
    measure_list_y = [1]

    steps_per_time_one = 5393761 / 5.4925

    steps_per_time_k = 38211305 / 14.0602
    actual_speedup = steps_per_time_k / steps_per_time_one 
    print(actual_speedup)
    measure_list_y.append(actual_speedup)

    steps_per_time_k = 21002732 / 6.0552
    actual_speedup = steps_per_time_k / steps_per_time_one 
    print(actual_speedup)
    measure_list_y.append(actual_speedup)

    steps_per_time_k = 17902410 / 4.4018
    actual_speedup = steps_per_time_k / steps_per_time_one 
    print(actual_speedup)
    measure_list_y.append(actual_speedup)

    steps_per_time_k = 83851666 / 18.1531
    actual_speedup = steps_per_time_k / steps_per_time_one 
    print(actual_speedup)
    measure_list_y.append(actual_speedup)

    steps_per_time_k = 19693904 / 6.5851
    actual_speedup = steps_per_time_k / steps_per_time_one 
    print(actual_speedup)
    measure_list_y.append(actual_speedup)


    title_string = 'Test done with accuracy goal %1.9f' % args.accuracy
    ax.set(xlabel='k', ylabel='Speedup', title=title_string)
    ax.grid()
    ax.plot(theory_list_x, theory_list_y, label='Theoretical speedup')
    ax.plot(measure_list_x, measure_list_y, label='Measured speedup')
    ax.legend(loc='upper left')
    plt.savefig(args.file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute Pi using Monte Carlo simulation.',
        epilog = 'Example: mp-pi-montecarlo-pool.py -s 100000 -w 4'
    )
    parser.add_argument('--accuracy', '-a',
                        default='0.999999',
                        type = float,
                        help='Number of steps in the Monte Carlo simulation')
    parser.add_argument('--file', '-f',
                        default = 'accuracy.png',
                        type = str,
                        help = 'Filename to save the graph to')
    args = parser.parse_args()
    print_speedup(args)

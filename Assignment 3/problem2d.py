#!/usr/bin/env python
#
# File: kmeans.py
# Author: Alexander Schliep (alexander@schlieplab.org)
#
#
import logging
import argparse
import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_blobs
import time
import multiprocessing as mp

def generateData(n, c):
    logging.info(f"Generating {n} samples in {c} classes")
    X, y = make_blobs(n_samples=n, centers = c, cluster_std=1.7, shuffle=False,
                      random_state = 2122)
    return X


def nearestCentroid(datum, centroids):
    # norm(a-b) is Euclidean distance, matrix - vector computes difference
    # for all rows of matrix
    dist = np.linalg.norm(centroids - datum, axis=1)
    return np.argmin(dist), np.min(dist)

# Function to assign data points to centroids.
# Used by the worker function that in turn can be parallelized.
def assignDataPoints(centroids, data, k):   
    N = len(data)
    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    c = np.zeros(N, dtype=int)

    # Assign data points to nearest centroid
    variation = np.zeros(k)
    cluster_sizes = np.zeros(k, dtype=int)
    for i in range(N):
        cluster, dist = nearestCentroid(data[i],centroids)
        c[i] = cluster
        cluster_sizes[cluster] += 1
        variation[cluster] += dist**2
    return c, variation, cluster_sizes

# Function to recompute centroids.
# Used by the worker function that in turn can be parallelized.
def recomputeCentroids(assignments, data, k):
    N = len(data)
    # Recompute centroids
    centroids = np.zeros((k,2)) # This fixes the dimension to 2
    for i in range(N):
        centroids[assignments[i]] += data[i]        
    return centroids

# Worker function that can be parallelized
# Exchanges data through queues with the kmeans function
def worker(job_queue, result_queue, data, k, i):
    while True:
        centroids = job_queue.get()
        c, variation, cluster_sizes = assignDataPoints(centroids, data, k)
        sums = recomputeCentroids(c, data, k)
        result_queue.put((sums, variation, cluster_sizes, c, i))


def kmeans(k, data, workers, nr_iter = 100):
    start = time.time()

    N = len(data)

    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    c = np.zeros(N, dtype=int)

    # Choose k random data points as centroids
    centroids = data[np.random.choice(np.array(range(N)),size=k,replace=False)]
    logging.debug("Initial centroids\n", centroids)

    # Queue for data sent to workers
    job_queue = mp.Queue()
    # Queue for results from workers
    result_queue = mp.Queue()

    # Create start end and ending indexes for splitting data and assignments between workers
    indexes = []
    for i in range(workers):
        indexes.append( ( int((N/workers)*i) , int((N/workers)*(i+1)-1 )) )
    # Since indexes are rounded down we add the remainder to the last worker
    # But no need if there is only one worker
    if (workers > 1):
        rest = N % workers
        indexes[:-1][0] = (indexes[:-1][0][0], indexes[:-1][0][1] + rest )

    # Processes are started here and loaded with slices of the data but are idling while waiting for centroids to be put on queue
    # An index is also sent to keep track on where the splice of data resides in the total data.
    jobs = []
    for i in range(workers):
        p = mp.Process(target=worker, daemon = True, args=(job_queue, result_queue, data[indexes[i][0]:indexes[i][1]], k, i))
        jobs.append(p)
        p.start()

    logging.info("Iteration\tVariation\tDelta Variation")
    total_variation = 0.0

    time_initial = time.time()  - start
    print("Time spent on initial work: %1.5f" % (time_initial))

    start = time.time()

    for j in range(nr_iter):
        logging.debug("=== Iteration %d ===" % (j+1))

        

        cluster_sizes = np.zeros(k, dtype=int)       

        # Send centroids to job queue to make workers start working.     
        for w in range(workers):
            job_queue.put(centroids)

        # Collect and process results from each worker
        centroids = np.zeros((k,2)) 
        old_variation = total_variation
        total_variation = 0.0
        for w in range(workers):
            sums, variation, c_sizes, assignments, index = result_queue.get() 
            # The result from each worker are summed up
            for i in range(k):
                centroids[i] += sums[i]
            # The same with cluster sizes
            for i in range(k):
                cluster_sizes[i] += c_sizes[i]
            # And total variation
            total_variation += sum(variation)
            # Assignments are saved but used only once K-means have finished so that we can plot the graph
            for t in range(len(assignments)):
                c[int((index* (N/workers) + t))] = assignments[t]
           
        delta_variation = -old_variation
        delta_variation += total_variation
        logging.info("%f\t%f" % (total_variation, delta_variation))
        
        # Had to implement a check to avoid dividing by zero since
        # sometimes a centroid was assigned zero data points.
        for i in range(k):
            if (cluster_sizes[i] > 0):
                centroids[i] = centroids[i] / cluster_sizes[i]
        
        logging.debug(cluster_sizes)
        logging.debug(c)
        logging.debug(centroids)

    time_parallel = time.time() - start
    print("Time spent on parallel work: %1.5f" % (time_parallel))

    start = time.time()
    
    # Terminating and then joining every working to avoid zombie processes
    for i in range(args.workers):
        jobs[i].terminate()
        logging.info("Terminated job %6d" % i)   

    for i in range(args.workers):
        jobs[i].join()
        logging.info("Joined job %6d" % i)   
   
    job_queue.close()
    logging.info("Closed assign queue")
    result_queue.close()
    logging.info("Closed assign result queue")


    job_queue.join_thread()
    logging.info("Joined assign queue")
    result_queue.join_thread()
    logging.info("Joined assign result queue")

    time_cleanup = time.time() - start
    print("Time spent on cleanup: %1.5f" % (time_cleanup))
    
    return total_variation, c


def computeClustering(args):
    if args.verbose:
        logging.basicConfig(format='# %(message)s',level=logging.INFO)
    if args.debug: 
        logging.basicConfig(format='# %(message)s',level=logging.DEBUG)

    
    X = generateData(args.samples, args.classes)

    start_time = time.time()
    #
    # Modify kmeans code to use args.worker parallel threads
    total_variation, assignment = kmeans(args.k_clusters, X, args.workers, nr_iter = args.iterations)
    #
    #
    total_time = time.time() - start_time
    logging.info("Clustering complete in %3.2f [s]" % (total_time))
    print(f"Total variation {total_variation}")
    print("Total time spent: %1.5f" % (total_time))
    

    if args.plot: # Assuming 2D data
        fig, axes = plt.subplots(nrows=1, ncols=1)
        axes.scatter(X[:, 0], X[:, 1], c=assignment, alpha=0.2)
        plt.title("k-means result")
        #plt.show()        
        fig.savefig(args.plot)
        plt.close(fig)
    return total_time

def print_speedup(args):

    fig, ax = plt.subplots()
    theory_list_x = []
    theory_list_y = []
    measure_list_x = []
    measure_list_y = []

    k = args.workers
    actual_speedup = k

    time_k_one = computeClustering(args)

    theory_list_x.append(k)
    theory_list_y.append(k)
    measure_list_x.append(k)
    measure_list_y.append(actual_speedup)

    k = k * 2
    args.workers = k

    while (k < 64):
        time_k = computeClustering(args)
        actual_speedup = time_k_one / time_k
        theory_list_x.append(k)
        theory_list_y.append(k)
        measure_list_x.append(k)
        measure_list_y.append(actual_speedup)
        k = k * 2
        args.workers = k
    ax.set(xlabel='k', ylabel='Speedup', title='Test done with 50000 samples and 500 iterations')
    ax.grid()
    ax.plot(theory_list_x, theory_list_y, label='Theoretical speedup')
    ax.plot(measure_list_x, measure_list_y, label='Measured speedup')
    ax.legend(loc='upper left')
    plt.savefig('kmeans_speedup.png')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Compute a k-means clustering.',
        epilog = 'Example: kmeans.py -v -k 4 --samples 10000 --classes 4 --plot result.png'
    )
    parser.add_argument('--workers', '-w',
                        default='1',
                        type = int,
                        help='Number of parallel processes to use (NOT IMPLEMENTED)')
    parser.add_argument('--k_clusters', '-k',
                        default='3',
                        type = int,
                        help='Number of clusters')
    parser.add_argument('--iterations', '-i',
                        default='100',
                        type = int,
                        help='Number of iterations in k-means')
    parser.add_argument('--samples', '-s',
                        default='10000',
                        type = int,
                        help='Number of samples to generate as input')
    parser.add_argument('--classes', '-c',
                        default='3',
                        type = int,
                        help='Number of classes to generate samples from')   
    parser.add_argument('--plot', '-p',
                        type = str,
                        help='Filename to plot the final result')   
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='Print verbose diagnostic output')
    parser.add_argument('--debug', '-d',
                        action='store_true',
                        help='Print debugging output')
    parser.add_argument('--speedup', '-m',
                        default = False,
                        action='store_true',
                        required=False,
                        help='Run automatic test for speedup graph or not.')
    args = parser.parse_args()
    if args.speedup is True:
        # Override any manual settings user might have set
        args.workers = 1
        args.samples = 50000
        print_speedup(args)
    else:
        computeClustering(args)


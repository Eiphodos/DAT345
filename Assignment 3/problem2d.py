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
def assignDataPoints(centroids, data):   
    N = len(data)
    k = len(centroids)
    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    c = np.zeros(N, dtype=int)

    # Assign data points to nearest centroid
    variation = np.zeros(k)
    total_variation = 0.0
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
# Exchanges data through several queues with the kmeans function
def worker(assign_queue, recompute_queue, assign_result_queue, recompute_result_queue):
    while True:
        centroids, data, i = assign_queue.get()
        c, variation, cluster_sizes = assignDataPoints(centroids, data)
        assign_result_queue.put((c, variation, cluster_sizes, i))
        assignments, data, k = recompute_queue.get()
        means = recomputeCentroids(assignments, data, k)
        recompute_result_queue.put(means)


def kmeans(k, data, workers, nr_iter = 100):
    N = len(data)

    # The cluster index: c[i] = j indicates that i-th datum is in j-th cluster
    c = np.zeros(N, dtype=int)

    # Choose k random data points as centroids
    centroids = data[np.random.choice(np.array(range(N)),size=k,replace=False)]
    logging.debug("Initial centroids\n", centroids)

    # Queue for data sent to workers to assign data to clusters
    assign_queue = mp.Queue()
    # Queue for data sent to workers to recompute centroids
    recompute_queue = mp.Queue()
    # Queue for results from workers when assigning data to clusters
    assign_result_queue = mp.Queue()
    # Queue for results from workers when recomputing centroids
    recompute_result_queue = mp.Queue()

    # Processes are started here but are idling while waiting for data to be put on queue
    jobs = []
    for i in range(workers):
        p = mp.Process(target=worker, args=(assign_queue, recompute_queue, assign_result_queue, recompute_result_queue))
        jobs.append(p)
        p.start()

    # Create start end and ending indexes for splitting data and assignments between workers
    indexes = []
    for i in range(workers):
        indexes.append( ( int((N/workers)*i) , int((N/workers)*(i+1)-1 )) )
    # Since indexes are rounded down we add the remainder to the last worker
    rest = N % workers
    indexes[:-1][0] = (indexes[:-1][0][0], indexes[:-1][0][1] + rest )

    logging.info("Iteration\tVariation\tDelta Variation")
    total_variation = 0.0

    for j in range(nr_iter):
        logging.debug("=== Iteration %d ===" % (j+1))
        
        cluster_sizes = np.zeros(k, dtype=int)       

        start = time.time()

        # Send centroids and data to assign queue to make workers start assigning data points.
        # An index is also sent to keep track on where the splice of data resides in the total data.
        for w in range(workers):
            assign_queue.put((centroids, data[indexes[w][0]:indexes[w][1]], w))
        
        # As workers are finished with assignments, process the results.
        # Minor bottleneck here where workers cant start working on recomputing centroids
        # until this is finished
        old_variation = total_variation
        total_variation = 0.0
        for w in range(workers):
            assignments, variation, c_sizes, i = assign_result_queue.get()
            for t in range(len(assignments)):
                c[int((i* (N/workers) + t))] = assignments[t]
            for i in range(k):
                cluster_sizes[i] += c_sizes[i]
            total_variation += sum(variation)
        delta_variation = -old_variation
        delta_variation += total_variation
        logging.info("%f\t%f" % (total_variation, delta_variation))

        time_assign = time.time() - start
        start = time.time()

        # Send data and assignments to workers to make them start recomputing centroids
        for w in range(workers):
            recompute_queue.put((c[indexes[w][0]:indexes[w][1]], data[indexes[w][0]:indexes[w][1]], k))
        
        # Collect and process results from recomputation
        centroids = np.zeros((k,2)) 
        for w in range(workers):
            result = recompute_result_queue.get() 
            for i in range(k):
                centroids[i] += result[i]
        centroids = centroids / cluster_sizes.reshape(-1,1)

        time_recompute = time.time() - start
        
        
        logging.debug(cluster_sizes)
        logging.debug(c)
        logging.debug(centroids)
    
    # A more sophisticated way to do this would be to send a message to each worker
    # from the estimator once accuracy has been reached that tells them to stop working
    # and then running join for each worker.
    for i in range(args.workers):
        jobs[i].terminate()
        logging.info("Terminated job %6d" % i)   
   
    assign_queue.close()
    logging.info("Closed assign queue")
    assign_result_queue.close()
    logging.info("Closed assign result queue")
    recompute_queue.close()
    logging.info("Closed recompute queue")
    recompute_result_queue.close()
    logging.info("Closed recompute result queue")

    assign_queue.join_thread()
    logging.info("Joined assign queue")
    assign_result_queue.join_thread()
    logging.info("Joined assign result queue")
    recompute_queue.join_thread()
    logging.info("Joined recompute queue")
    recompute_result_queue.join_thread()
    logging.info("Joined recompute result queue")
    
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
    end_time = time.time()
    logging.info("Clustering complete in %3.2f [s]" % (end_time - start_time))
    print(f"Total variation {total_variation}")

    if args.plot: # Assuming 2D data
        fig, axes = plt.subplots(nrows=1, ncols=1)
        axes.scatter(X[:, 0], X[:, 1], c=assignment, alpha=0.2)
        plt.title("k-means result")
        #plt.show()        
        fig.savefig(args.plot)
        plt.close(fig)

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
    computeClustering(args)


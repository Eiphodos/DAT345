from pyspark import SparkContext
import argparse
import math

def produceStats(args):
    # Initialize Spark
    sc = SparkContext("local[%d]" % args.cores)
    data = sc.textFile(args.file)

    # Gather all values
    values = data.map(lambda l: l.split()).map(lambda l: float(l[2]))
    # Count the elements
    lines = values.count()
    # Calculate min and max
    maximum = values.reduce(lambda a, b: max(a, b))
    minimum = values.reduce(lambda a, b: min(a, b))
    # Create a histogram
    buckets, bucketcounts = values.histogram(10)
    # Calculate mean to use for mean and standard deviation
    mean = values.reduce(lambda a, b: a + b) / lines
    # Calculate mean and standard deviation
    std_dev = math.sqrt(values.map(lambda l: (l - mean)**2).reduce(lambda a, b: a + b) / lines)
    mean_dev = values.map(lambda l: abs(l - mean)).reduce(lambda a, b: a + b) / lines
    # Calculate median
    median = values.sortBy(lambda l: l).collect()[int(lines/2)]


    # Print results
    print("Maximum : %1.6f" % maximum)
    print("Minimum : %1.6f" % minimum)
    print("Mean deviation: %1.6f" % mean_dev)
    print("Standard deviation: %1.6f" % std_dev)
    print("Buckets")
    print("Bucket 1 = %1.6f <= X < %1.6f : %d" % (buckets[0], buckets[1], bucketcounts[0]))
    print("Bucket 2 = %1.6f <= X < %1.6f : %d" % (buckets[1], buckets[2], bucketcounts[1]))
    print("Bucket 3 = %1.6f <= X < %1.6f : %d" % (buckets[2], buckets[3], bucketcounts[2]))
    print("Bucket 4 = %1.6f <= X < %1.6f : %d" % (buckets[3], buckets[4], bucketcounts[3]))
    print("Bucket 5 = %1.6f <= X < %1.6f : %d" % (buckets[4], buckets[5], bucketcounts[4]))
    print("Bucket 6 = %1.6f <= X < %1.6f : %d" % (buckets[5], buckets[6], bucketcounts[5]))
    print("Bucket 7 = %1.6f <= X < %1.6f : %d" % (buckets[6], buckets[7], bucketcounts[6]))
    print("Bucket 8 = %1.6f <= X < %1.6f : %d" % (buckets[7], buckets[8], bucketcounts[7]))
    print("Bucket 9 = %1.6f <= X < %1.6f : %d" % (buckets[8], buckets[9], bucketcounts[8]))
    print("Bucket 10 = %1.6f <= X <= %1.6f : %d" % (buckets[9], buckets[10], bucketcounts[9]))
    print("Median : %1.6f" % median)

            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Produce stats from data with Spark',
        epilog = 'Example: problem1a.py --file data.dat'
    )
    parser.add_argument('--file', '-f',
                        type = str,
                        help='File to process')
    parser.add_argument('--cores', '-c',
                        type = int,
                        default = 1,
                        help='Number of cores')
    args = parser.parse_args()
    produceStats(args)
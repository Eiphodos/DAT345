from pyspark import SparkContext
import argparse

sc = SparkContext(master = 'local[4]')

def produceStats(args):
    data = sc.textFile(args.file)

    values = data.map(lambda l: l.split()).map(lambda l: float(l[2]))
    maximum = values.reduce(lambda a, b: max(a, b))
    minimum = values.reduce(lambda a, b: min(a, b))
    bin5 = values.map(lambda l: int(l)).filter(lambda l: 5 <= l and l < 6).count()
    print("Maximum : %1.6f" % maximum)
    print("Minimum : %1.6f" % minimum)
    print("Bin 5: %d" % bin5)
    


            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Produce stats from data with Spark',
        epilog = 'Example: problem1a.py --file data.dat'
    )
    parser.add_argument('--file', '-f',
                        type = str,
                        help='file to process')
    args = parser.parse_args()
    produceStats(args)
from mrjob.job import MRJob, MRStep
import sys
import time
import math

class Problem1a(MRJob):

    def mean_dev(self, value, mean):
        return abs(value - mean)

    def mapper(self, _, line):
        splitline = line.split()
        start = time.time()
        group = splitline[1]
        value = float(splitline[2])
        yield ("lineinfo", (value, start))
    
    def combiner(self, key, counts):
        minimum = sys.float_info.max
        maximum = -sys.float_info.max
        partial_sum = 0
        partial_sum_squared = 0
        bins = [0] * 10
        values = []
        start = 0
        for i, c in enumerate(counts):
            values.append(c[0])
            partial_sum += c[0]
            partial_sum_squared += c[0] ** 2
            if c[0] < minimum:
                minimum = c[0]
            if c[0] > maximum:
                maximum = c[0]
            bindex = int(c[0])
            bins[bindex] += 1
            start = c[1]
        nr_lines = i +1
        yield ("stats", (nr_lines, partial_sum, partial_sum_squared, values, start))
        yield ("bins", (bins, start))
        yield ("minimum", (minimum, start))
        yield ("maximum", (maximum, start))
    

    def reducer(self, key, counts):
        if key == "stats":
            total_lines = 0
            total_sum = 0
            total_sum_squared = 0
            final_max = 0
            all_values = []

            for c in counts:
                total_lines += c[0]
                total_sum += c[1]
                total_sum_squared += c[2]
                all_values += c[3]
                start = c[4]

            mean = float(total_sum)/total_lines
            stand_dev = math.sqrt((total_sum_squared - (total_sum * total_sum)/total_lines)/(total_lines-1))
            yield("Standard deviation: ", stand_dev)

            mean_deviation = sum([self.mean_dev(value, mean) for value in all_values]) / len(all_values)
            yield("Mean deviation: ", mean_deviation)

            yield("Total time dev: ", time.time() - start)
            

        if key == "bins":
            final_bins = [0] * 10
            for c in counts:
                start = c[1]
                for i, b in enumerate(c[0]):
                    final_bins[i] += b

            yield ("X < 1\t", final_bins[0])
            yield ("1 <= X < 2", final_bins[1])
            yield ("2 <= X < 3", final_bins[2])
            yield ("3 <= X < 4", final_bins[3])
            yield ("4 <= X < 5", final_bins[4])
            yield ("5 <= X < 6", final_bins[5])
            yield ("6 <= X < 7", final_bins[6])
            yield ("7 <= X < 8", final_bins[7])
            yield ("8 <= X < 9", final_bins[8])
            yield ("9 <= X", final_bins[9])

            yield("Total time bin: ", time.time() - start)
        
        if key == "minimum":
            final_min = sys.float_info.max
            for c in counts:
                start = c[1]
                if c[0] < final_min:
                    final_min = c[0]
            yield ("Minimum: ", final_min)

            yield("Total time min: ", time.time() - start)
        
        if key == "maximum":
            final_max = -sys.float_info.max
            for c in counts:
                start = c[1]           
                if c[0] > final_max:
                    final_max = c[0]
            yield ("Maximum: ", final_max)

            yield("Total time max: ", time.time() - start)
            
if __name__ == '__main__':
    Problem1a.run()
from mrjob.job import MRJob, MRStep
import sys
import time
import os
import tempfile
import math

class Problem1a(MRJob):
    
    def steps(self):      
        return [MRStep( mapper=self.mapper,
                        combiner=self.combiner,
                        reducer=self.reducer),
                        MRStep(mapper=self.mean_dev_mapper,
                                combiner=self.mean_dev_combiner,
                                reducer=self.mean_dev_reducer)]
    

    def mapper(self, _, line):
        splitline = line.split()
        group = splitline[1]
        value = float(splitline[2])
        start = time.time()
        yield ("lineinfo", (value, start))
    
    def combiner(self, key, counts):
        minimum = sys.float_info.max
        maximum = -sys.float_info.max
        partial_sum = 0
        bins = [0] * 10
        values = []
        for i, c in enumerate(counts):
            values.append(c[0])
            partial_sum += c[0]
            if c[0] < minimum:
                minimum = c[0]
            if c[0] > maximum:
                maximum = c[0]
            bindex = int(c[0])
            bins[bindex] += 1
            start = c[1]
        nr_lines = i +1
        yield ("stats", (nr_lines, partial_sum, values, start))
        yield ("bins", bins)
        yield ("minimum", minimum)
        yield ("maximum", maximum)
    

    def reducer(self, key, counts):
        if key == "stats":
            total_lines = 0
            total_sum = 0
            all_values = []
            for c in counts:
                total_lines += c[0]
                total_sum += c[1]
                all_values += c[2]
                start = c[3]

            mean = float(total_sum)/total_lines

            for v in all_values:
                yield("deviation", (mean, v, start))
            

        if key == "bins":
            final_bins = [0] * 10
            for c in counts:
                for i, b in enumerate(c):
                    final_bins[i] += b
            yield ("bins", (final_bins[0], final_bins[1], final_bins[2], final_bins[3], final_bins[4],
                             final_bins[5], final_bins[6], final_bins[7], final_bins[8], final_bins[9]))
        
        if key == "minimum":
            final_min = sys.float_info.max
            for c in counts:
                if c < final_min:
                    final_min = c
            yield ("min", final_min)
        
        if key == "maximum":
            final_max = -sys.float_info.max
            for c in counts:           
                if c > final_max:
                    final_max = c
            yield ("max", final_max)

    def mean_dev_mapper(self, key, line):
        if key == "deviation":
            mean, value, start = line
            deviation = abs(value - mean)
            std_deviation = (value - mean) ** 2
            yield("deviation", (deviation, std_deviation, start))
        if key == "stddev":
            yield(key, line)
        if key == "bins":
            yield(key, line)
        if key == "max":
            yield(key, line)
        if key == "min":
            yield(key, line)

    def mean_dev_combiner(self, key, counts):
        if key == "deviation":
            partial_sum_mean = 0
            partial_sum_std = 0
            for i, c in enumerate(counts):
                partial_sum_mean += c[0]
                partial_sum_std += c[1]
                start = c[2]
            nr_lines = i +1
            yield ("dev_stats", (nr_lines, partial_sum_mean, partial_sum_std, start))
        if key == "stddev":
            yield(key, counts)
        if key == "bins":
            yield(key, counts)
        if key == "max":
            yield(key, counts)
        if key == "min":
            yield(key, counts)

    def mean_dev_reducer(self, key, counts):
        if key == "dev_stats":
            total_lines = 0
            total_sum_mean = 0
            total_sum_std = 0
            for c in counts:
                total_lines += c[0]
                total_sum_mean += c[1]
                total_sum_std += c[2]
                start = c[3]

            mean_dev = float(total_sum_mean)/total_lines
            std_dev = math.sqrt(float(total_sum_std)/total_lines)
            yield("Mean deviation: ", mean_dev)
            yield("Standard deviation: ", std_dev)
            yield("Total time: ", time.time() - start) 

        if key == "bins":
            final_bins = next(counts)[0]
            yield ("X < 1", final_bins[1])
            yield ("1 <= X < 2", final_bins[1])
            yield ("2 <= X < 3", final_bins[2])
            yield ("3 <= X < 4", final_bins[3])
            yield ("4 <= X < 5", final_bins[4])
            yield ("5 <= X < 6", final_bins[5])
            yield ("6 <= X < 7", final_bins[6])
            yield ("7 <= X < 8", final_bins[7])
            yield ("8 <= X < 9", final_bins[8])
            yield ("9 <= X", final_bins[9])
        if key == "max":
            final_min = next(counts)[0]
            yield ("Minimum: ", final_min)
        if key == "min":
            final_max = next(counts)[0]
            yield ("Maximum: ", final_max)
        
if __name__ == '__main__':
    Problem1a.run()
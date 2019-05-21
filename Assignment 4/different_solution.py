from mrjob.job import MRJob, MRStep
import sys
import time
import os
import tempfile

class Problem1a(MRJob):

    def std_dev(self, value, mean):
        return (value - mean)**2
    
    def mean_dev(self, value, mean):
        return abs(value - mean)
    '''
    def steps(self):      
        return [MRStep( mapper=self.mapper,
                        combiner=self.combiner,
                        reducer=self.reducer),
                        MRStep(reducer=self.result)]
    ''' 

    def mapper(self, _, line):
        splitline = line.split()
        group = splitline[1]
        value = float(splitline[2])
        yield ("lineinfo", value)
    
    def combiner(self, key, counts):
        minimum = sys.float_info.max
        maximum = -sys.float_info.max
        partial_sum = 0
        partial_sum_squared = 0
        bins = [0] * 10
        values = []
        for i, c in enumerate(counts):
            values.append(c)
            partial_sum += c
            partial_sum_squared += c ** 2
            if c < minimum:
                minimum = c
            if c > maximum:
                maximum = c
            bindex = int(c)
            bins[bindex] += 1
        nr_lines = i +1
        yield ("stats", (nr_lines, partial_sum, partial_sum_squared, values))
        yield ("bins", bins)
        yield ("minimum", minimum)
        yield ("maximum", maximum)
    

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

            mean = float(total_sum)/total_lines
            stand_dev = (total_sum_squared - (total_sum * total_sum)/total_lines)/(total_lines-1)
            yield("Standard deviation: ", stand_dev)

            mean_deviation = sum([self.mean_dev(value, mean) for value in all_values]) / len(all_values)
            yield("Mean deviation: ", mean_deviation)
            

        if key == "bins":
            final_bins = [0] * 10
            for c in counts:
                for i, b in enumerate(c):
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
        
        if key == "minimum":
            final_min = sys.float_info.max
            for c in counts:
                if c < final_min:
                    final_min = c
            yield ("Minimum: ", final_min)
        
        if key == "maximum":
            final_max = -sys.float_info.max
            for c in counts:           
                if c > final_max:
                    final_max = c
            yield ("Maximum: ", final_max)
            
if __name__ == '__main__':
    Problem1a.run()
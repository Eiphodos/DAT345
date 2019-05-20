from mrjob.job import MRJob, MRStep
import time

class Problem1a(MRJob):

    def std_dev(self, value, mean):
        return (value - mean)**2
    
    def mean_dev(self, value, mean):
        return abs(value - mean)

    def steps(self):
        return [MRStep( mapper=self.mapper,
                        combiner=self.combiner,
                        reducer=self.reducer),
                        MRStep(reducer=self.results)]
    
    def mapper(self, _, line):
        splitline = line.split()
        identifier = splitline[0]
        group = splitline[1]
        value = float(splitline[2])
        yield ("lineinfo", (identifier, value, group))

    def combiner(self, key, counts):
        minimum = 100000
        maximum = 0
        partial_sum = 0
        bins = [0] * 10
        values = []
        for i, c in enumerate(counts):
            values.append(c[1])
            partial_sum += c[1]
            if c[1] < minimum:
                minimum = c[1]
            if c[1] > maximum:
                maximum = c[1]
            bindex = int(c[1])
            bins[bindex] += 1
        nr_lines = i +1
        yield ("stats", (nr_lines, partial_sum, values))
        yield ("bins", bins)
        yield ("minimum", minimum)
        yield ("maximum", maximum)

    def reducer(self, key, counts):
        if key == "stats":
            total_lines = 0
            total_sum = 0
            final_max = 0
            all_values = []
            for c in counts:
                total_lines += c[0]
                total_sum += c[1]
                all_values += c[2]

            mean = float(total_sum)/total_lines
            yield ("standarddev", (mean, all_values))
            yield ("meandev", (mean, all_values))

        if key == "bins":
            final_bins = [0] * 10
            for c in counts:
                for i, b in enumerate(c):
                    final_bins[i] += b
            yield ("bins", final_bins)
        
        if key == "minimum":
            final_min = 100000
            for c in counts:
                if c < final_min:
                    final_min = c
            yield ("minimum", final_min)
        
        if key == "maximum":
            final_max = 0
            for c in counts:           
                if c > final_max:
                    final_max = c
            yield ("maximum", final_max)

    def results(self, key, result):
        if key == "standarddev":
            mean, all_values = next(result)
            standard_dev = sum([self.std_dev(value, mean) for value in all_values]) / len(all_values)
            yield("Standard deviation: ", standard_dev)
        if key == "meandev":
            mean, all_values = next(result)
            mean_deviation = sum([self.mean_dev(value, mean) for value in all_values]) / len(all_values)
            yield("Mean deviation: ", mean_deviation)
        if key == "bins":
            bins = next(result)
            yield ("X < 1\t", bins[0])
            yield ("1 <= X < 2", bins[1])
            yield ("2 <= X < 3", bins[2])
            yield ("3 <= X < 4", bins[3])
            yield ("4 <= X < 5", bins[4])
            yield ("5 <= X < 6", bins[5])
            yield ("6 <= X < 7", bins[6])
            yield ("7 <= X < 8", bins[7])
            yield ("8 <= X < 9", bins[8])
            yield ("9 <= X", bins[9])
        if key == "minimum":
            yield ("Minimum: ", result)
        if key == "maximum":
            yield ("Maximum: ", result)

if __name__ == '__main__':
    start = time.time()
    Problem1a.run()
    total_time = time.time() - start
    print("Total time: %1.6f" % total_time)
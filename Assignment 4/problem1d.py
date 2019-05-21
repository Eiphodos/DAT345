from mrjob.job import MRJob, MRStep
import sys
import time
import math

class Problem1a(MRJob):

    def configure_args(self):
        super(Problem1a, self).configure_args()
        self.add_passthru_arg('--group', '-g',
                             default=-1,
                             type=int,
                             required=False,
                             help='Specify group number to run statistics for')


    def mean_std_dev(self, value, mean):
        mean_dev = abs(value - mean)
        std_dev = (value - mean)**2
        return mean_dev, std_dev

    def mapper_init(self):
        self.group_choice = self.options.group

    def mapper(self, _, line):
        splitline = line.split()
        group = int(splitline[1])
        value = float(splitline[2])
        #If group is the default value, process all data
        if self.group_choice == -1:
            yield ("lineinfo", value)
        #Else we only process the data that belongs to the specified group
        elif self.group_choice == group:
            yield ("lineinfo", value)

        
    def combiner(self, key, counts):
        minimum = sys.float_info.max
        maximum = -sys.float_info.max
        partial_sum = 0
        bins = [0] * 10
        values = []
        for i, c in enumerate(counts):
            values.append(c)
            partial_sum += c
            if c < minimum:
                minimum = c
            if c > maximum:
                maximum = c
            bindex = int(c)
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


            # I would like to avoid having to pass all values to the reducer and looping through them here
            # But I was unable to find a good solution

            dev_list = [self.mean_std_dev(value, mean) for value in all_values]

            # I tried another approach where I had an additional step of map/combine/reduce
            # Where the new mapper would do calculate a deviation on a single value with the mean from the first reducer
            # Combiner would do the partial sums for the std and mean deviations
            # and the final second reducer to summarize and calculate the final mean and std deviations
            # The min/max/bins values would just pass through their values without touching them.
            # That double m/c/r version was slower than this version by a factor of about 10 so I gave it up.
            
            # I experimented a bit with single pass algorithms for standard dev and mean dev and while 
            # I got that working for standard dev, I could not find any good algo for mean dev which meant I had to
            # do an additional pass over the data anyway.

            # This version was still very fast, 24 seconds on single core and 6 seconds on 32 cores on the compute server
            # to complete full calculations on the entire dataset (excluding time to setup tmp files and cleanup). 

            mean_devs, std_devs = zip(*dev_list)
            stand_dev = math.sqrt(sum(std_devs)/total_lines)
            mean_deviation = sum(mean_devs)/total_lines

            yield("Standard deviation: ", stand_dev)
            yield("Mean deviation: ", mean_deviation)

        if key == "bins":
            final_bins = [0] * 10
            
            for c in counts:
                for i, b in enumerate(c):
                    final_bins[i] += b
            yield ("X < 1", final_bins[0])
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
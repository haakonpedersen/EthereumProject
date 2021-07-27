"""JOB 3 - TOP TEN

Finally, the third job will take as input the now filtered address aggregates
and sort these via a top ten reducer, utilising what you have learned
from lab 4.
"""

from mrjob.job import MRJob


class TopMiners(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split('\t')
            if len(fields) == 2:
                to_address = fields[0]
                value = float(fields[1])
                yield (None, (to_address, value))
        except :
            pass

    def combiner(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield ("top", value)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, values):
        sorted_values = sorted(values, reverse = True, key = lambda tup:tup[1])
        i = 0
        for value in sorted_values:
            yield i, (" = {} - {}".format(value[0],value[1]))
            i += 1
            if i >= 10:
                break


if __name__ == '__main__':
    TopMiners.run()

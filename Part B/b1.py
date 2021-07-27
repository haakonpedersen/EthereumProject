"""JOB 1 - INITIAL AGGREGATION

To workout which services are the most popular, you will first have to aggregate
transactions to see how much each address within the user space has been involved in.
You will want to aggregate value for addresses in the to_address field. This will be
similar to the wordcount that we saw in Lab 1 and Lab 2.
"""

from mrjob.job import MRJob


class EtherPartB1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                to_address = fields[2]
                value = float(fields[3])
                if value == 0:
                    pass
                else:
                    yield (to_address, value)
        except:
            pass

    def combiner (self, to_address, value):
            yield (to_address, sum(value))

    def reducer(self, to_address, value):
            yield (to_address, sum(value))

if __name__ == '__main__':
    EtherPartB1.run()

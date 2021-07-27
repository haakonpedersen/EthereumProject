"""Evaluate the top 10 miners by the size of the blocks mined. This is simpler
as it does not require a join. You will first have to aggregate blocks to see
how much each miner has been involved in. You will want to aggregate size for
addresses in the miner field. This will be similar to the wordcount that we saw
in Lab 1 and Lab 2. You can add each value from the reducer to a list and then
sort the list to obtain the most active miners."""

from mrjob.job import MRJob


class EtherPartC1(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 9:
                miner = fields[2]
                size = float(fields[4])
                yield (miner, size)
        except:
            pass

    def combiner (self, miner, sizes):
            yield (miner, sum(sizes))

    def reducer(self, miner, sizes):
            yield (miner, sum(sizes))

if __name__ == '__main__':
    EtherPartC1.run()

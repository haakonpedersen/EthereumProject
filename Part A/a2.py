"""Create a bar plot showing the average value of transaction in each month
between the start and end of the dataset."""

from mrjob.job import MRJob
import time


class EtherPartA2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                time_epoch = int(fields[6])
                month = time.strftime("%b %Y", time.gmtime(time_epoch))
                amount = float(fields[3])
                if float(amount) != 0:
                    yield (month, amount)
        except:
            pass

    def combiner (self, month, amount):
            yield (month, sum(amount))

    def reducer(self, month, amount):
            yield (month, sum(amount))

if __name__ == '__main__':
    EtherPartA2.run()

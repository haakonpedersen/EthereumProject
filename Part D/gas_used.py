
from mrjob.job import MRJob
import time

class Gas_Used(MRJob):

        def mapper(self, _, line):
                try:
                        fields = line.split(',')
                        if len(fields) == 7 :
                                time_epoch = int(fields[6])
                                month = time.strftime("%b %Y", time.gmtime(time_epoch))
                                gas_used = float(fields[4])

                                yield (month, (gas_used, 1))
                except:
                        pass

        def combiner(self, month, values):
                gas_used_total = 0
                transaction_count = 0
                for value in values:
                        gas_used_total += value[0]
                        transaction_count += value[1]

                yield (month, (gas_used_total, transaction_count))

        def reducer(self, month, values):
                gas_used_total = 0
                transaction_count = 0
                for value in values:
                        gas_used_total += value[0]
                        transaction_count += value[1]

                yield(month, (gas_used_total/transaction_count))

if __name__ == '__main__':
    Gas_Used.run()

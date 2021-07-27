"""JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING

Once you have obtained this aggregate of the transactions, the next step is to
perform a repartition join between this aggregate and contracts (example here).
You will want to join the to_address field from the output of Job 1 with the
address field of contracts Secondly, in the reducer, if the address for a given
aggregate from Job 1 was not present within contracts this should be filtered
out as it is a user address and not a smart contract.
"""

from mrjob.job import MRJob


class repartition_join(MRJob):

    def mapper(self, _, line):
        try:
            if len(line.split(','))==5:
                fields=line.split(',')
                join_key=fields[0]
                join_value=float(fields[3])

                yield (join_key,(join_value,1))

            elif len(line.split('\t'))==2:
                fields=line.split('\t')
                join_key=fields[0]
                join_key=join_key[1:-1]
                join_value=float(fields[1])

                yield (join_key,(join_value,2))
        except:
            pass

    def reducer(self, to_address, values):
        id = 0
        ovalue = 0

        for value in values:
            if value[1]==1:
                id=value[0]
            elif value[1]==2:
               ovalue = value[0]
        if id !=0 and ovalue !=0:
            yield (to_address, ovalue)

if __name__=='__main__':
    repartition_join.run()


#! /usr/bin/python3
# 4StatsT.py

from mrjob.job import MRJob, MRStep

class SummariseJob(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        key = 0
        AWND = float(fields[2])
        PRCP = float(fields[3])
        TAVG = (float(fields[4]) - 32)/1.8 #Convert temperature to Celsius
        TMAX = (float(fields[5]) - 32)/1.8
        TMIN = (float(fields[6]) - 32)/1.8
        yield key, (AWND,PRCP,TAVG,TMAX,TMIN)

    def reducer(self, key, values):

        values = list(values)

        AWND = [v[0] for v in values]
        PRCP = [v[1] for v in values]
        TAVG = [v[2] for v in values]
        TMAX = [v[3] for v in values]
        TMIN = [v[4] for v in values]

        def median(vals):
            val_sort = sorted(vals)
            n = int(len(val_sort) / 2)
            if len(vals) % 2:
                return (val_sort)[n-1:n+1]
            else:
                return val_sort[n]

        def quantile(vals,Q): #define function for calculating quantiles
            val_sort = sorted(vals)
            if type(Q) is list:
                Y = []
                for q in Q:
                    n = int(len(val_sort)*q)
                    Y.append(val_sort[n])
            else:
                 n = int(len(val_sort)*Q)
                 Y = val_sort[n]
            return Y

        def mean(vals):
            return sum(vals)/len(vals)
        
        def variance(vals):
            mean = sum(vals) / len(vals)
            deviations = [mean ** 2 for i in vals]
            if len(deviations)>1:
                return (sum(deviations) / (len(deviations) - 1))
            else:
                return 0
        def standard_deviation(vals):
            return variance(vals) ** 0.5

        def mode(vals):
            return max(set(vals), key=vals.count)

        def summary(vals):
            Y = []
            Y.append(min(vals))
            Y = quantile(vals,[0.01,0.05,0.1,0.25,0.3,0.4,0.5,0.6,0.75,0.8,0.9,0.95,0.99])
            Y.append(max(vals))           
            return (Y)

        AWND_t = summary(AWND)
        PRCP_t = summary(PRCP)
        TAVG_t = summary(TAVG)
        TMAX_t = summary(TMAX)
        TMIN_t = summary(TMIN)
        output = [o for o in AWND_t] + [o for o in PRCP_t] + [o for o in TAVG_t] + [o for o in TMAX_t] + [o for o in TMIN_t] 

        yield None, output


if __name__ == '__main__':
    SummariseJob.run()

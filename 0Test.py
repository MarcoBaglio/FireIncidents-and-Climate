! ~/Project
# 0-Test.py

from mrjob.job import MRJob

STATION_HEADER = 'STATION,"NAME","DATE","AWND","PRCP","TAVG","TMAX","TMIN"'
INCIDENTS_HEADER = ('alarmtime,year,incidentnum,exp_no,incidentcode,incitypedesc,indicentdesc,majorcategory,'
'streetaddress,mutl_aid,station,shift,current_district,current_fmz,latitude,longitude,geopoint')

FIELD_SEP = ','

class ReduceSideJoinJob(MRJob):

    def mapper(self, _, line):

        # Skip the header lines in both files
        if line != STATION_HEADER and line != INCIDENTS_HEADER:
            fields = line.split(FIELD_SEP)
            if len(fields) == 8: # We have the STATION dataset

                key = fields[3] # The key is in the attribute "DATA"
                PRCP = fields[4]
                TAVG = fields[5]

                value = (PRCP,TAVG)

                yield key, ('ST', value)

            elif len(fields) == 17: # We have the INCIDENTS dataset
                key = fields[0][:10] # The key is in the attribute alarmtime, we take the first ten characters
                IncidentType = fields[5]
                Lat = fields[-3]
                Long = fields[-2]

                value = (IncidentType,Lat,Long)
                yield key, ('IN', value)
            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):


        yield key, values


if __name__ == '__main__':
    ReduceSideJoinJob.run()

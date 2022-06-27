#! /usr/bin/python3
# 2TimeJoin_average.py

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep

STATION_HEADER = '"STATION";"NAME";"DATE";"AWND";"PRCP";"TAVG";"TMAX";"TMIN"'
INCIDENTS_HEADER = ('alarmtime;year;incidentnum;exp_no;incidentcode;incitypedesc;indicentdesc;majorcategory;'
'streetaddress;mutl_aid;station;shift;current_district;current_fmz;latitude;longitude;geopoint')

class ReduceSideJobEx(MRJob):
    def steps(self):

        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer
            ),
            MRStep(mapper=self.Mean_Mapper,
                   combiner=self.Mean_Combiner,
                   reducer=self.Mean_Reducer)
        ]
    OUTPUT_PROTOCOL = JSONValueProtocol # The reducer will output only the value, not the key

    def mapper(self, _, line):
        if line != STATION_HEADER and line != INCIDENTS_HEADER:
            fields = line.split(';')
            if len(fields) == 8: # STATION dataset
                key = fields[2] # key is in attribute DATE
                #Name = fields[1]
                AWND = fields[3]
                PRCP = fields[4]
                TAVG = fields[5]
                TMAX = fields[6]
                TMIN = fields[7]

                value = (AWND,PRCP,TAVG,TMAX,TMIN)
                yield key, ('ST',value)

            elif len(fields) == 17: # INCIDENTS dataset
                key = fields[0][:10] # The key is in the attribute alarmtime, we take the first ten characters
                Date = key.split('-')
                key = Date[2] + '/' + Date[1] + '/' + Date[0]
                IncidentType = fields[5]
                ID = fields[2]
                value = (IncidentType,ID)

                yield key, ('IN',value)

            else:
                raise ValueError('An input file does not contain the required number of fields.')

    def reducer(self, key, values):
        STATION_tuples = []
        INCIDENTS_tuples = []

        for value in list(values):
            relation = value[0]  # either 'ST' or 'IN'
            if relation == 'ST':  # station data
                #Name = value[1][0]
                AWND = value[1][0]
                PRCP = value[1][1]
                TAVG = value[1][2]
                TMAX = value[1][3]
                TMIN = value[1][4]
                STATION_tuples.append((AWND,PRCP,TAVG,TMAX,TMIN))
            elif relation == 'IN':  # incidents data
                IncidentType = value[1][0]
                ID = value[1][1]
                INCIDENTS_tuples.append((IncidentType,ID))
            else:
                raise ValueError('An unexpected join key was encountered.')

        if len(STATION_tuples) > 0 and len(INCIDENTS_tuples) > 0:
            for STATION_values in STATION_tuples:
                for INCIDENTS_values in INCIDENTS_tuples:
                    output = [o for o in STATION_values] + [o for o in INCIDENTS_values]
                    #TAVG = STATION_values[3]
                    #ID = INCIDENTS_values[1]
                    yield key, output
                    
    def Mean_Mapper(self,date,values):
        key = values[6] #assign incident ID as new key
        #TAVG = values[3]
        yield key, (date,(values))

    def Mean_Combiner(self, key , values):
        AWND_t = AWND_c = PRCP_t = PRCP_c = TAVG_t = TAVG_c = TMAX_t = TMAX_c = TMIN_t = TMIN_c = 0
        values = list(values)
        date = values[0][0]
        IType = values[0][1][5]
        for value in values:
            AWND = value[1][0]
            PRCP = value[1][1]
            TAVG = value[1][2]
            TMAX = value[1][3]
            TMIN = value[1][4]
            if AWND != '':
                AWND_t += float(AWND)
                AWND_c += 1
            if PRCP != '':
                PRCP_t += float(PRCP)
                PRCP_c += 1
            if TAVG != '':
                TAVG_t += float(TAVG)
                TAVG_c += 1
            if TMAX != '':
                TMAX_t += float(TMAX)
                TMAX_c += 1
            if TMIN != '':
                TMIN_t += float(TMIN)
                TMIN_c += 1
        yield key, (date,(AWND_t,AWND_c,PRCP_t,PRCP_c,TAVG_t,TAVG_c,TMAX_t,TMAX_c,TMIN_t,TMIN_c),IType)

     def Mean_Reducer(self,key,values):
        values = list(values)
        AWND_t = AWND_c = PRCP_t = PRCP_c = TAVG_t = TAVG_c = TMAX_t = TMAX_c = TMIN_t = TMIN_c = 0
        date = values[0][0]
        IType = values[0][2]
        for value in values:
             AWND_t += value[1][0]
             AWND_c += value[1][1]

             PRCP_t += value[1][2]
             PRCP_c += value[1][3]

             TAVG_t += value[1][4]
             TAVG_c += value[1][5]

             TMAX_t += value[1][6]
             TMAX_c += value[1][7]

             TMIN_t += value[1][8]
             TMIN_c += value[1][9]

        AWND_a = AWND_t/AWND_c
        PRCP_a = PRCP_t/PRCP_c
        TAVG_a = TAVG_t/TAVG_c
        TMAX_a = TMAX_t/TMAX_c
        TMIN_a = TMIN_t/TMIN_c
        output = [key] + [date] + [AWND_a] + [PRCP_a] + [TAVG_a] + [TMAX_a] + [TMIN_a] + [IType]
        yield  None, output
    
            

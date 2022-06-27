#! /usr/bin/python3
# 1TimeJoin.py

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

STATION_HEADER = '"STATION";"NAME";"DATE";"AWND";"PRCP";"TAVG";"TMAX";"TMIN"'
INCIDENTS_HEADER = ('alarmtime;year;incidentnum;exp_no;incidentcode;incitypedesc;indicentdesc;majorcategory;'
'streetaddress;mutl_aid;station;shift;current_district;current_fmz;latitude;longitude;geopoint')

class ReduceSideJobEx(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol # The reducer will output only the value, not the key

    # Allow the job to receive an argument from the command line
    # This argument will be used to specify the type of join
    def configure_args(self):
        super(ReduceSideJobEx, self).configure_args()
        self.add_passthru_arg(
            '--join_type',
            default = 'inner',
            choices=['inner', 'left_outer','right_outer'],
            help="The type of join"
        )

    def mapper(self, _, line):
        if line != STATION_HEADER and line != INCIDENTS_HEADER:

            fields = line.split(';')
            if len(fields) == 8: # STATION dataset
                key = fields[2] # key is in attribute DATE
                Name = fields[1]
                AWND = fields[3]
                PRCP = fields[4]
                TAVG = fields[5]
                TMAX = fields[6]
                TMIN = fields[7]

                value = (Name,AWND,PRCP,TAVG,TMAX,TMIN) # UnitPrice times Quantity
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
                STR = len(fields)
                #raise ValueError('An input file does not contain the required number of fields.')
                raise ValueError(STR)
    def reducer(self, key, values):

        STATION_tuples = []
        INCIDENTS_tuples = []

        for value in list(values):
            relation = value[0]  # either 'ST' or 'IN'
            if relation == 'ST':  # station data
                Name = value[1][0]
                AWND = value[1][1]
                PRCP = value[1][2]
                TAVG = value[1][3]
                TMAX = value[1][3]
                TMIN = value[1][4]

                STATION_tuples.append((Name,AWND,PRCP,TAVG,TMAX,TMIN))
            elif relation == 'IN':  # incidents data
                IncidentType = value[1][0]
                ID = value[1][1]
                INCIDENTS_tuples.append((IncidentType,ID))
            else:
                raise ValueError('An unexpected join key was encountered.')
        if self.options.join_type == 'inner':
            if len(STATION_tuples) > 0 and len(INCIDENTS_tuples) > 0:
                for STATION_values in STATION_tuples:
                    for INCIDENTS_values in INCIDENTS_tuples:
                        output = [key] + [o for o in STATION_values] + [o for o in INCIDENTS_values]
                        yield key, output
        elif self.options.join_type == 'left_outer':
            if len(order_tuples) > 0:
                for order_values in order_tuples:
                    if len(detail_tuples) > 0:
                        count = len(detail_tuples)
                        total = sum(detail_tuples)
                        output = [key] + [o for o in order_values] + [count, total]
                    else:
                        output = [key] + [o for o in order_values] + ['null','null']
                    yield None, output
        elif self.options.join_type == 'right_outer':
            if len(detail_tuples) > 0:
                count = len(detail_tuples)
                total = sum(detail_tuples)
                if len(order_tuples) > 0:
                    for order_values in order_tuples:
                        output = [key] + [o for o in order_values] + [count, total]
                        yield None, output
                else:
                    output = [key] + ['null','null'] + [count, total]
                    yield None, output
        else:
            raise ValueError('An unexpected join type was encountered. This should not happen.')

if __name__ == '__main__':
    ReduceSideJobEx.run()

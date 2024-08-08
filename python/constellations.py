#!/usr/bin/python3

# This module makes it easy to unpack statistics data that was
# serialized to the database using SimDB's "constellations"
# feature.
#
# It is provided as a python module to help keep the internal
# details of this feature hidden from users, or at least not
# required that users interact directly with the internally-
# created schema tables to support stats collection.

import argparse, sqlite3, zlib, struct

class Constellations:
    # Create with the full path to the database file
    def __init__(self, db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT TimeType FROM ConstellationGlobals LIMIT 1')
        for row in cursor.fetchall():
            self._time_type = row[0]
        
        cursor.execute('SELECT Id,Name,DataType,Compressed FROM Constellations')

        meta_by_id = {}
        for id, name, data_type, compressed in cursor.fetchall():
            meta_by_id[id] = {'Name':name,
                              'DataType':data_type,
                              'Compressed':compressed,
                              'StatPaths':[]}

        cursor.execute('SELECT ConstellationID,StatPath FROM ConstellationPaths')
        for id,stat_path in cursor.fetchall():
            meta_by_id[id]['StatPaths'].append(stat_path)

        self._constellations = []
        for id, meta in meta_by_id.items():
            name = meta['Name']
            data_type = meta['DataType']
            compressed = meta['Compressed']
            stat_paths = meta['StatPaths']

            constellation = Constellation(conn, id, name, self._time_type, data_type, compressed, stat_paths)
            self._constellations.append(constellation)

    @property
    def time_type(self):
        return self._time_type

    # Get all constellation data (time and data values) as a dict:
    #
    # all_constellations = {
    #   'ConstellationA': {
    #     'TimeVals': [1,2,3,4,5]
    #     'DataVals': {
    #       'stat1': [5,2,6,7,4],
    #       'stat2': [7,7,3,4,1]
    #     }
    #   },
    #   'ConstellationB': {
    #     ...
    #   }
    # }
    def Unpack(self, constellation_name=None, time_range=None):
        all_constellations = {}
        for constellation in self._constellations:
            if constellation_name is None or constellation_name == constellation.name:
                all_constellations[constellation.name] = constellation.Unpack(time_range)

        if constellation_name is not None:
            valid_names = [const.name for const in self._constellations]
            assert len(all_constellations) > 0, \
                'Constellation named {} does not exist. ' \
                'Available constellations: {}'.format(constellation_name, \
                                                      ','.join(valid_names))

        return all_constellations

    # Dump all constellations (data and metadata) to the provided CSV file:
    #
    # ConstellationA
    # TimeVals,1,2,3,4,5
    # stat1,5,2,6,7,4
    # stat2,7,7,3,4,1
    #
    # ConstellationB
    # ...
    #
    def DumpToCSV(self, filename, constellation_name=None, time_range=None, time_precision=None, data_precision=None):
        all_constellations = self.Unpack(constellation_name, time_range)

        def DumpConstellation(name, data_dict, time_precision, data_precision, fout):
            fout.write('Constellation,' + name + ',\n')
            fout.write('TimeVals,')
            time_vals = data_dict['TimeVals']
            if time_precision >= 0:
                time_vals = [round(val, time_precision) for val in time_vals]
            fout.write(','.join([str(val) for val in time_vals]) + ',\n')

            for stat_path, stat_data in data_dict['DataVals'].items():
                fout.write(stat_path + ',')
                if data_precision >= 0:
                    stat_data = [round(val, data_precision) for val in stat_data]
                fout.write(','.join([str(val) for val in stat_data]) + ',\n')

            fout.write('\n')

        with open(filename, 'w') as fout:
            for name, data_dict in all_constellations.items():
                DumpConstellation(name, data_dict, time_precision, data_precision, fout)

class Constellation:
    def __init__(self, conn, id, name, time_type, data_type, compressed, stat_paths):
        self._conn = conn
        self._id = id
        self._name = name
        self._time_type = time_type
        self._data_type = data_type
        self._compressed = compressed
        self._stat_paths = stat_paths

    @property
    def name(self):
        return self._name

    # Get constellation data (time and data values) as a dict:
    #
    # constellation = {
    #   'TimeVals': [1,2,3,4,5]
    #   'DataVals': {
    #     'stat1': [5,2,6,7,4],
    #     'stat2': [7,7,3,4,1]
    #   }
    # }
    def Unpack(self, time_range=None):
        cursor = self._conn.cursor()
        cmd = 'SELECT TimeVal,DataVals FROM ConstellationData WHERE ConstellationID={} '.format(self._id)

        if time_range is not None:
            assert type(time_range) in (list,tuple) and len(time_range) == 2
            if time_range[0] >= 0:
                cmd += 'AND TimeVal>={} '.format(time_range[0])
            if time_range[1] >= 0:
                cmd += 'AND TimeVal<={} '.format(time_range[1])

        cursor.execute(cmd)

        time_vals = []
        data_matrix = []

        for time_val, data_vals in cursor.fetchall():
            time_vals.append(self._FormatTimeVal(time_val))
            data_vals = self._FormatDataVals(data_vals)
            data_matrix.append(data_vals)

        for row in data_matrix:
            assert len(self._stat_paths) == len(row)

        # The current form of the data_matrix is:
        #
        # [s1a,s2a,s3a,...]    # All stats in order at time0
        # [s1b,s2b,s3b,...]    # All stats in order at time1
        # [s1c,s2c,s3c,...]    # All stats in order at time2
        #
        # We need this to be of the form:
        #
        # [s1a,s1b,s1c,...]    # All stat1 values for time0,1,2,...
        # [s2a,s2b,s2c,...]    # All stat2 values for time0,1,2,...
        # [s3a,s3b,s3c,...]    # All stat3 values for time0,1,2,...

        assert len(data_matrix) == len(time_vals)
        data_matrix = [[x[i] for x in data_matrix] for i in range(len(data_matrix[0]))]

        # constellation = {
        #   'TimeVals': [1,2,3,4,5]
        #   'DataVals': {
        #     'stat1': [5,2,6,7,4],
        #     'stat2': [7,7,3,4,1]
        #   }
        # }

        constellation_dict = {'TimeVals':time_vals, 'DataVals':{}}
        for i,stat_path in enumerate(self._stat_paths):
            constellation_dict['DataVals'][stat_path] = data_matrix[i]

        return constellation_dict

    def _FormatTimeVal(self, time_val):
        if self._time_type == 'INT':
            return int(time_val)
        elif self._time_type == 'REAL':
            return float(time_val)
        else:
            return time_val

    def _FormatDataVals(self, data_vals):
        # data_vals start off as raw bytes (C chars) whether
        # we have to decompress or not.
        if self._compressed:
            data_vals = zlib.decompress(data_vals)

        # Unpack the raw chars into real world values e.g. uint32_t,  double, etc.
        if self._data_type == 'uint8_t':
            fmt = 'B'*len(data_vals)
        elif self._data_type == 'uint16_t':
            fmt = 'H'*(int(len(data_vals)/2))
        elif self._data_type == 'uint32_t':
            fmt = 'I'*(int(len(data_vals)/4))
        elif self._data_type == 'uint64_t':
            fmt = 'Q'*(int(len(data_vals)/8))
        elif self._data_type == 'int8_t':
            fmt = 'b'*len(data_vals)
        elif self._data_type == 'int16_t':
            fmt = 'h'*(int(len(data_vals)/2))
        elif self._data_type == 'int32_t':
            fmt = 'i'*(int(len(data_vals)/4))
        elif self._data_type == 'int64_t':
            fmt = 'q'*(int(len(data_vals)/8))
        elif self._data_type == 'float':
            fmt = 'f'*(int(len(data_vals)/4))
        elif self._data_type == 'double':
            fmt = 'd'*(int(len(data_vals)/8))
        else:
            raise ValueError('Invalid data type: ' + self._data_type)

        return struct.unpack(fmt, data_vals)

def ParseArgs(time_type=str):
    parser = argparse.ArgumentParser()

    parser.add_argument('--db-file', required=True, help='Path to the database file')

    parser.add_argument('--csv-report-file', help='Filename of CSV report')

    parser.add_argument('--time-precision', type=int, default=-1, help='Precision of the time values in report files')

    parser.add_argument('--data-precision', type=int, default=-1, help='Precision of the data values in report files')

    parser.add_argument('--constellation-name', help='Name of a single constellation to write to the report(s)')

    parser.add_argument('--time-range', nargs='+', type=time_type,
                        help='Range of time values to print to the report(s) as ' \
                        '"10 400" (everything between time 10 and time 400) or ' \
                        '"-1 400" (everything up to time 400) or ' \
                        '"10 -1" (everything from time 10 onward)')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = ParseArgs()
    db_path = args.db_file
    constellations = Constellations(db_path)
    time_type = constellations.time_type

    if time_type == 'INT':
        args = ParseArgs(time_type=int)
    elif time_type == 'REAL':
        args = ParseArgs(time_type=float)
    else:
        raise ValueError('Invalid time datatype, should be INT or REAL: ' + time_type)

    time_precision = args.time_precision
    data_precision = args.data_precision
    constellation_name = args.constellation_name
    time_range = args.time_range

    if args.csv_report_file:
        constellations.DumpToCSV(args.csv_report_file, constellation_name, time_range, time_precision, data_precision)

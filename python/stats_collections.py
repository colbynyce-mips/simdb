#!/usr/bin/python3

# This module makes it easy to unpack statistics data that was
# serialized to the database using SimDB's "collections"
# feature.
#
# It is provided as a python module to help keep the internal
# details of this feature hidden from users, or at least not
# required that users interact directly with the internally-
# created schema tables to support stats collection.

import argparse, sqlite3, zlib, struct

class Collections:
    # Create with the full path to the database file
    def __init__(self, db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT TimeType FROM CollectionGlobals LIMIT 1')
        for row in cursor.fetchall():
            self._time_type = row[0]
        
        cursor.execute('SELECT Id,Name,DataType,Compressed FROM Collections')

        meta_by_id = {}
        for id, name, data_type, compressed in cursor.fetchall():
            meta_by_id[id] = {'Name':name,
                              'DataType':data_type,
                              'Compressed':compressed,
                              'StatPaths':[]}

        cursor.execute('SELECT CollectionID,StatPath FROM CollectionPaths')
        for id,stat_path in cursor.fetchall():
            meta_by_id[id]['StatPaths'].append(stat_path)

        self._collections = []
        for id, meta in meta_by_id.items():
            name = meta['Name']
            data_type = meta['DataType']
            compressed = meta['Compressed']
            stat_paths = meta['StatPaths']

            collection = Collection(conn, id, name, self._time_type, data_type, compressed, stat_paths)
            self._collections.append(collection)

    @property
    def time_type(self):
        return self._time_type

    # Get all collection data (time and data values) as a dict:
    #
    # all_collections = {
    #   'CollectionA': {
    #     'TimeVals': [1,2,3,4,5]
    #     'DataVals': {
    #       'stat1': [5,2,6,7,4],
    #       'stat2': [7,7,3,4,1]
    #     }
    #   },
    #   'CollectionB': {
    #     ...
    #   }
    # }
    def Unpack(self, collection_name=None, time_range=None):
        all_collections = {}
        for collection in self._collections:
            if collection_name is None or collection_name == collection.name:
                all_collections[collection.name] = collection.Unpack(time_range)

        if collection_name is not None:
            valid_names = [const.name for const in self._collections]
            assert len(all_collections) > 0, \
                'Collection named {} does not exist. ' \
                'Available collections: {}'.format(collection_name, \
                                                      ','.join(valid_names))

        return all_collections

    # Dump all collections (data and metadata) to the provided CSV file:
    #
    # CollectionA
    # TimeVals,1,2,3,4,5
    # stat1,5,2,6,7,4
    # stat2,7,7,3,4,1
    #
    # CollectionB
    # ...
    #
    def DumpToCSV(self, filename, collection_name=None, time_range=None, time_precision=None, data_precision=None):
        all_collections = self.Unpack(collection_name, time_range)

        def DumpCollection(name, data_dict, time_precision, data_precision, fout):
            fout.write('Collection,' + name + ',\n')
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
            for name, data_dict in all_collections.items():
                DumpCollection(name, data_dict, time_precision, data_precision, fout)

class Collection:
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

    # Get collection data (time and data values) as a dict:
    #
    # collection = {
    #   'TimeVals': [1,2,3,4,5]
    #   'DataVals': {
    #     'stat1': [5,2,6,7,4],
    #     'stat2': [7,7,3,4,1]
    #   }
    # }
    def Unpack(self, time_range=None):
        cursor = self._conn.cursor()
        cmd = 'SELECT TimeVal,DataVals FROM CollectionData WHERE CollectionID={} '.format(self._id)

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

        # collection = {
        #   'TimeVals': [1,2,3,4,5]
        #   'DataVals': {
        #     'stat1': [5,2,6,7,4],
        #     'stat2': [7,7,3,4,1]
        #   }
        # }

        collection_dict = {'TimeVals':time_vals, 'DataVals':{}}
        for i,stat_path in enumerate(self._stat_paths):
            collection_dict['DataVals'][stat_path] = data_matrix[i]

        return collection_dict

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

    parser.add_argument('--collection-name', help='Name of a single collection to write to the report(s)')

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
    collections = Collections(db_path)
    time_type = collections.time_type

    if time_type == 'INT':
        args = ParseArgs(time_type=int)
    elif time_type == 'REAL':
        args = ParseArgs(time_type=float)
    else:
        raise ValueError('Invalid time datatype, should be INT or REAL: ' + time_type)

    time_precision = args.time_precision
    data_precision = args.data_precision
    collection_name = args.collection_name
    time_range = args.time_range

    if args.csv_report_file:
        collections.DumpToCSV(args.csv_report_file, collection_name, time_range, time_precision, data_precision)

#!/usr/bin/python3

# This module makes it easy to unpack statistics data that was
# serialized to the database using SimDB's collection feature.
#
# It is provided as a python module to help keep the internal
# details of this feature hidden from users, or at least not
# required that users interact directly with the internally-
# created schema tables to support collection.

import argparse, sqlite3, zlib, struct

class Collections:
    def __init__(self, db_path):
        self._conn = sqlite3.connect(db_path)
        cursor = self.cursor()

        cursor.execute('SELECT TimeType FROM CollectionGlobals LIMIT 1')
        for row in cursor.fetchall():
            self._time_type = row[0]

        cursor.execute('SELECT Id,Name,DataType,IsContainer FROM Collections')
        meta_by_collection_id = {}
        self._collection_names_by_collection_id = {}
        for id,name,dtype,is_container in cursor.fetchall():
            self._collection_names_by_collection_id[id] = name
            meta_by_collection_id[id] = {'Name':name,
                                         'DataType':dtype,
                                         'IsContainer':is_container,
                                         'Elements':[]}

        cursor.execute('SELECT PathID,Capacity,IsSparse FROM ContainerMeta')
        container_meta_by_path_id = {}
        for path_id,capacity,is_sparse in cursor.fetchall():
            container_meta_by_path_id[path_id] = {'Capacity':capacity, 'IsSparse':is_sparse}

        cursor.execute('SELECT Id,CollectionID,SimPath FROM CollectionElems')
        self._collection_names_by_simpath = {}
        self._collection_ids_by_simpath = {}
        for path_id,collection_id,simpath in cursor.fetchall():
            meta_by_collection_id[collection_id]['Elements'].append(simpath)
            self._collection_names_by_simpath[simpath] = self._collection_names_by_collection_id[collection_id]
            self._collection_ids_by_simpath[simpath] = collection_id

        self._offsets_by_simpath = {}
        for collection_id,meta in meta_by_collection_id.items():
            for i,simpath in enumerate(meta['Elements']):
                self._offsets_by_simpath[simpath] = i

        cursor.execute('SELECT IntVal,String FROM StringMap')
        strings_by_int = {}
        for intval,stringval in cursor.fetchall():
            strings_by_int[intval] = stringval

        cursor.execute('SELECT EnumName,EnumValStr,EnumValBlob,IntType FROM EnumDefns')
        enums_by_name = {}
        for enum_name,enum_str,enum_blob,int_type in cursor.fetchall():
            if enum_name not in enums_by_name:
                enums_by_name[enum_name] = Enum(enum_name, int_type)

            enums_by_name[enum_name].AddEntry(enum_str, enum_blob)

        cursor.execute('SELECT Name FROM Collections WHERE IsContainer=1')
        container_collections = set()
        for collection_name in cursor.fetchall():
            container_collections.add(collection_name[0])

        cursor.execute('SELECT PathID,Capacity,IsSparse FROM ContainerMeta')
        container_meta_by_path_id = {}
        for path_id,capacity,is_sparse in cursor.fetchall():
            container_meta_by_path_id[path_id] = {'Capacity':capacity, 'IsSparse':is_sparse}

        cursor.execute('SELECT Id,SimPath FROM CollectionElems')
        container_meta_by_simpath = {}
        for path_id,simpath in cursor.fetchall():
            if path_id in container_meta_by_path_id:
                container_meta_by_simpath[simpath] = container_meta_by_path_id[path_id]

        cursor.execute('SELECT CollectionName,FieldName,FieldType,FormatCode FROM StructFields')
        self._deserializers_by_collection_name = {}
        for collection_name,field_name,field_type,format_code in cursor.fetchall():
            if collection_name not in self._deserializers_by_collection_name:
                if collection_name not in container_collections:
                    deserializer = StructDeserializer(strings_by_int, enums_by_name, self._offsets_by_simpath, cursor)
                else:
                    deserializer = IterableDeserializer(strings_by_int, enums_by_name, container_meta_by_simpath, self._offsets_by_simpath, cursor)

                self._deserializers_by_collection_name[collection_name] = deserializer

            deserializer = self._deserializers_by_collection_name[collection_name]
            deserializer.AddField(field_name, field_type, format_code)

        cmd = 'SELECT Name,DataType FROM Collections WHERE IsContainer=0 AND DataType IN ({})'
        pods = ['int8_t','int16_t','int32_t','int64_t','uint8_t','uint16_t','uint32_t','uint64_t','double','float']
        cmd = cmd.format(','.join('"'+pod+'"' for pod in pods))
        cursor.execute(cmd)

        for collection_name,data_type in cursor.fetchall():
            assert collection_name not in self._deserializers_by_collection_name
            self._deserializers_by_collection_name[collection_name] = StatsDeserializer(data_type, self._offsets_by_simpath, cursor)

    def cursor(self):
        return self._conn.cursor()

    # Get all collected data for the given element by its path. These are the
    # same paths that were used in the original calls to addStat(), addStruct(),
    # and addContainer().
    def Unpack(self, elem_path, time_range=None):
        cursor = self.cursor()
        collection_id = self._collection_ids_by_simpath[elem_path]
        cmd = 'SELECT Id,TimeVal,DataVals FROM CollectionData WHERE CollectionID={} '.format(collection_id)

        if time_range is not None:
            assert type(time_range) in (list,tuple) and len(time_range) == 2
            if time_range[0] >= 0:
                cmd += 'AND TimeVal>={} '.format(time_range[0])
            if time_range[1] >= 0:
                cmd += 'AND TimeVal<={} '.format(time_range[1])

        cursor.execute(cmd)

        time_vals = []
        data_vals = []

        collection_name = self._collection_names_by_simpath[elem_path]
        deserializer = self._deserializers_by_collection_name[collection_name]

        for collection_data_id, time_val, data_blob in cursor.fetchall():
            time_vals.append(self._FormatTimeVal(time_val))
            if data_blob is None or len(data_blob) == 0:
                data_vals.append(None)
            else:
                data_blob = zlib.decompress(data_blob)
                data_vals.append(deserializer.Unpack(data_blob, elem_path, collection_data_id))

        return {'TimeVals': time_vals, 'DataVals': data_vals}

    def _FormatTimeVal(self, time_val):
        if self._time_type == 'INT':
            return int(time_val)
        elif self._time_type == 'REAL':
            return float(time_val)
        else:
            return time_val

class Enum:
    def __init__(self, name, int_type):
        self._name = name
        self._strings_by_int = {}

        if int_type == 'int8_t':
            self._format =  'b'
        elif int_type == 'int16_t':
            self._format =  'h'
        elif int_type == 'int32_t':
            self._format =  'i'
        elif int_type == 'int64_t':
            self._format =  'q'
        elif int_type == 'uint8_t':
            self._format =  'B'
        elif int_type == 'uint16_t':
            self._format =  'H'
        elif int_type == 'uint32_t':
            self._format =  'I'
        elif int_type == 'uint64_t':
            self._format =  'Q'
        else:
            raise ValueError('Invalid enum integer type: ' + int_type)

    @property
    def format(self):
        return self._format

    def AddEntry(self, enum_string, enum_blob):
        int_val = struct.unpack(self._format, enum_blob)[0]
        self._strings_by_int[int_val] = enum_string

    def Format(self, val):
        return self._strings_by_int[val]

class Deserializer:
    def __init__(self, cursor):
        self._cursor = cursor

    @property
    def cursor(self):
        return self._cursor

class StatsDeserializer(Deserializer):
    NUM_BYTES_MAP = {
        'int8_t'  :1,
        'int16_t' :2,
        'int32_t' :4,
        'int64_t' :8,
        'uint8_t' :1,
        'uint16_t':2,
        'uint32_t':4,
        'uint64_t':8,
        'float'   :4,
        'double'  :8
    }

    FORMAT_CODES_MAP = {
        'int8_t'  :'b',
        'int16_t' :'h',
        'int32_t' :'i',
        'int64_t' :'q',
        'uint8_t' :'B',
        'uint16_t':'H',
        'uint32_t':'I',
        'uint64_t':'Q',
        'float'   :'f',
        'double'  :'d'
    }

    def __init__(self, data_type, offsets_by_simpath, cursor):
        Deserializer.__init__(self, cursor)
        self._scalar_num_bytes = StatsDeserializer.NUM_BYTES_MAP[data_type]
        self._format = StatsDeserializer.FORMAT_CODES_MAP[data_type]
        self._cast = float if data_type in ('float','double') else int
        self._offsets_by_simpath = offsets_by_simpath

    def Unpack(self, data_blob, elem_path, collection_data_id):
        elem_offset = self._offsets_by_simpath[elem_path]
        start = elem_offset*self._scalar_num_bytes
        end = start + self._scalar_num_bytes
        raw_bytes = data_blob[start:end]
        val = struct.unpack(self._format, raw_bytes)[0]
        return self._cast(val)

class StructDeserializer(Deserializer):
    def __init__(self, strings_by_int, enums_by_name, offsets_by_simpath, cursor):
        Deserializer.__init__(self, cursor)
        self._strings_by_int = strings_by_int
        self._enums_by_name = enums_by_name
        self._offsets_by_simpath = offsets_by_simpath
        self._field_formatters = []
        self._format = ''

    def AddField(self, field_name, field_type, format_code):
        unpack_format_codes_by_builtin_dtype = {
            'char_t':'c',
            'int8_t':'b',
            'int16_t':'h',
            'int32_t':'i',
            'int64_t':'q',
            'uint8_t':'B',
            'uint16_t':'H',
            'uint32_t':'I',
            'uint64_t':'Q',
            'float_t':'f',
            'double_t':'d',
        }

        if field_type in unpack_format_codes_by_builtin_dtype:
            formatter = BuiltinFormatter(field_name, format_code)
            self._format += unpack_format_codes_by_builtin_dtype[field_type]
        elif field_type == 'string_t':
            formatter = MappedStringFormatter(field_name, self._strings_by_int)
            self._format += 'i'
        else:
            formatter = EnumFormatter(field_name, self._enums_by_name[field_type])
            self._format += self._enums_by_name[field_type].format

        self._field_formatters.append(formatter)

    def Unpack(self, data_blob, elem_path, collection_data_id):
        struct_num_bytes = self.GetStructNumBytes()
        elem_offset = self._offsets_by_simpath[elem_path]
        data_blob = data_blob[struct_num_bytes*elem_offset:struct_num_bytes*(elem_offset+1)]

        num_bytes_by_format_code = {
            'c':1,
            'b':1,
            'h':2,
            'i':4,
            'q':8,
            'B':1,
            'H':2,
            'I':4,
            'Q':8,
            'f':4,
            'd':8
        }

        res = {}
        for i,code in enumerate(self._format):
            nbytes = num_bytes_by_format_code[code]
            tiny_blob = data_blob[:nbytes]
            data_blob = data_blob[nbytes:]
            val = struct.unpack(code, tiny_blob)[0]
            formatter = self._field_formatters[i]
            res[formatter.field_name] = formatter.Format(val)

        assert len(data_blob) == 0
        return res

    def GetStructNumBytes(self):
        num_bytes_by_format_code = {
            'c':1,
            'b':1,
            'h':2,
            'i':4,
            'q':8,
            'B':1,
            'H':2,
            'I':4,
            'Q':8,
            'f':4,
            'd':8
        }

        num_bytes = 0
        for code in self._format:
            num_bytes += num_bytes_by_format_code[code]

        return num_bytes

class IterableDeserializer(StructDeserializer):
    def __init__(self, strings_by_int, enums_by_name, container_meta_by_simpath, offsets_by_simpath, cursor):
        StructDeserializer.__init__(self, strings_by_int, enums_by_name, offsets_by_simpath, cursor)
        self._container_meta_by_simpath = container_meta_by_simpath
        self._offsets_by_simpath = offsets_by_simpath

    def Unpack(self, data_blob, elem_path, collection_data_id):
        offset = self._offsets_by_simpath[elem_path]
        capacity = self._container_meta_by_simpath[elem_path]['Capacity']
        sparse = self._container_meta_by_simpath[elem_path]['IsSparse']
        struct_num_bytes = self.GetStructNumBytes()

        res = []

        if not sparse:
            while len(data_blob) > 0:
                struct_blob = data_blob[:struct_num_bytes]
                data_blob = data_blob[struct_num_bytes:]
                res.append(StructDeserializer.Unpack(self, struct_blob, elem_path, collection_data_id))
        else:
            cmd = 'SELECT Flags FROM SparseValidFlags WHERE CollectionDataID={}'.format(collection_data_id)
            cursor = self.cursor
            cursor.execute(cmd)

            flags = None
            for flags in cursor.fetchall():
                flags = flags[0]

            assert flags is not None and len(flags) > 0
            flags = zlib.decompress(flags)
            flags = struct.unpack('i'*int(len(data_blob) / struct_num_bytes), flags)
            assert len(flags) == len(data_blob) / struct_num_bytes

            flag_idx = 0
            while len(data_blob) > 0:
                struct_blob = data_blob[:struct_num_bytes]
                data_blob = data_blob[struct_num_bytes:]
                if flags[flag_idx]:
                    res.append(StructDeserializer.Unpack(self, struct_blob, elem_path, collection_data_id))
                else:
                    res.append(None)

                flag_idx += 1

        return res

class Formatter:
    def __init__(self, field_name):
        self._field_name = field_name

    @property
    def field_name(self):
        return self._field_name

class BuiltinFormatter(Formatter):
    def __init__(self, field_name, format_code):
        Formatter.__init__(self, field_name)
        self._format_code = format_code

    def Format(self, val):
        if isinstance(val, bytes):
            val = val.decode('utf-8')
        if self._format_code == 1:
            return hex(val)
        else:
            return val

class MappedStringFormatter(Formatter):
    def __init__(self, field_name, strings_by_int):
        Formatter.__init__(self, field_name)
        self._strings_by_int = strings_by_int

    def Format(self, val):
        return self._strings_by_int[val]

class EnumFormatter(Formatter):
    def __init__(self, field_name, enum_handler):
        Formatter.__init__(self, field_name)
        self._enum_handler = enum_handler

    def Format(self, val):
        return self._enum_handler.Format(val)

def ParseArgs():
    parser = argparse.ArgumentParser()

    parser.add_argument('--db-file', required=True, help='Path to the database file')

    parser.add_argument('--element-path', help='Path of a single element to unpack')

    parser.add_argument('--time-range', nargs='+', type=float,
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

    elem_path = args.element_path
    time_range = args.time_range
    print (collections.Unpack(elem_path, time_range))

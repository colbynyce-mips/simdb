import zlib, struct, copy
from viewer.gui.view_settings import DirtyReasons

class DataRetriever:
    def __init__(self, frame, db, simhier):
        self.frame = frame
        self._db = db
        self.cursor = db.cursor()
        cursor = self.cursor

        cursor.execute('SELECT TimeType,Heartbeat FROM CollectionGlobals')
        self._time_type, self._heartbeat = cursor.fetchone()

        cursor.execute('SELECT Id,Name,DataType,IsContainer,IsSparse,Capacity FROM Collections')
        meta_by_collection_id = {}
        self._collection_names_by_collection_id = {}
        for id,name,dtype,is_container,is_sparse,capacity in cursor.fetchall():
            self._collection_names_by_collection_id[id] = name
            meta_by_collection_id[id] = {'Name':name,
                                         'DataType':dtype,
                                         'IsContainer':is_container,
                                         'Elements':[],
                                         'IsSparse':is_sparse,
                                         'Capacity':capacity}

        container_meta_by_elem_path = {}
        for elem_path in simhier.GetItemElemPaths():
            collection_id = simhier.GetCollectionID(elem_path)
            meta = copy.deepcopy(meta_by_collection_id[collection_id])
            del meta['Name']
            del meta['Elements']
            container_meta_by_elem_path[elem_path] = meta_by_collection_id[collection_id]

        self._collection_names_by_elem_path = {}
        for elem_path in simhier.GetItemElemPaths():
            collection_id = simhier.GetCollectionID(elem_path)
            self._collection_names_by_elem_path[elem_path] = self._collection_names_by_collection_id[collection_id]

        self._collection_ids_by_elem_path = {}
        for elem_path in simhier.GetItemElemPaths():
            collection_id = simhier.GetCollectionID(elem_path)
            self._collection_ids_by_elem_path[elem_path] = collection_id

        self._element_idxs_by_elem_path = {}
        cursor.execute('SELECT Id,CollectionOffset FROM ElementTreeNodes')
        for elem_id,offset in cursor.fetchall():
            elem_path = simhier.GetElemPath(elem_id)
            self._element_idxs_by_elem_path[elem_path] = offset

        cursor.execute('SELECT IntVal,String FROM StringMap')
        strings_by_int = {}
        for intval,stringval in cursor.fetchall():
            strings_by_int[intval] = stringval

        cursor.execute('SELECT EnumName,EnumValStr,EnumValBlob,IntType FROM EnumDefns')
        enums_by_name = {}
        for enum_name,enum_str,enum_blob,int_type in cursor.fetchall():
            if enum_name not in enums_by_name:
                enums_by_name[enum_name] = EnumDef(enum_name, int_type)

            enums_by_name[enum_name].AddEntry(enum_str, enum_blob)

        cursor.execute('SELECT DISTINCT(StructName) FROM StructFields')
        struct_names = []
        for struct_name in cursor.fetchall():
            struct_names.append(struct_name[0])

        cursor.execute('SELECT Name,IsContainer FROM Collections WHERE DataType IN ({})'.format(','.join('"'+name+'"' for name in struct_names)))
        iterable_struct_collection_names = set()
        scalar_struct_collection_names = set()
        for collection_name,is_container in cursor.fetchall():
            if is_container:
                iterable_struct_collection_names.add(collection_name)
            else:
                scalar_struct_collection_names.add(collection_name)

        cursor.execute('SELECT Id,Name,DataType,IsContainer FROM Collections WHERE DataType IN ({})'.format(','.join('"'+name+'"' for name in struct_names)))
        self._deserializers_by_collection_name = {}

        for collection_id,collection_name,struct_name,is_container in cursor.fetchall():
            assert collection_name not in self._deserializers_by_collection_name
            if struct_name not in struct_names:
                continue

            if collection_name in scalar_struct_collection_names:
                deserializer = StructDeserializer(self, struct_name, strings_by_int, enums_by_name, self._element_idxs_by_elem_path, cursor)
                self._deserializers_by_collection_name[collection_name] = deserializer
            elif collection_name in iterable_struct_collection_names:
                deserializer = IterableDeserializer(self, struct_name, strings_by_int, enums_by_name, container_meta_by_elem_path, self._element_idxs_by_elem_path, cursor)
                self._deserializers_by_collection_name[collection_name] = deserializer

            cursor.execute('SELECT FieldName,FieldType,FormatCode FROM StructFields WHERE StructName="{}"'.format(struct_name))
            for field_name,field_type,format_code in cursor.fetchall():
                deserializer.AddField(field_name, field_type, format_code)

        self.ResetToDefaultViewSettings(False)

        cmd = 'SELECT Name,DataType FROM Collections WHERE IsContainer=0 AND DataType IN ({})'
        pods = ['int8_t','int16_t','int32_t','int64_t','uint8_t','uint16_t','uint32_t','uint64_t','double','float','bool']
        cmd = cmd.format(','.join('"'+pod+'"' for pod in pods))
        cursor.execute(cmd)

        for collection_name,data_type in cursor.fetchall():
            assert collection_name not in self._deserializers_by_collection_name
            self._deserializers_by_collection_name[collection_name] = StatsDeserializer(data_type, self._element_idxs_by_elem_path, cursor)

        cmd = 'SELECT TimeVal FROM CollectionData'
        cursor.execute(cmd)

        self._time_vals = []
        for time_val in cursor.fetchall():
            self._time_vals.append(self._FormatTimeVal(time_val[0]))

        cmd = 'SELECT Id,IsSparse FROM Collections'
        cursor.execute(cmd)

        self._is_sparse_by_collection_id = {}
        for collection_id,is_sparse in cursor.fetchall():
            self._is_sparse_by_collection_id[collection_id] = is_sparse

        self._cached_utiliz_time_val = None
        self._cached_utiliz_sizes = None

        # Sanity checks to ensure that no element path contains 'root.'
        for elem_path,_ in container_meta_by_elem_path.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._collection_names_by_elem_path.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._collection_ids_by_elem_path.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._element_idxs_by_elem_path.items():
            assert elem_path.find('root.') == -1

    def GetTimeType(self):
        return self._time_type

    def GetCurrentViewSettings(self):
        settings = {}
        assert set(self._displayed_columns_by_struct_name.keys()) == set(self._auto_colorize_column_by_struct_name.keys())

        for struct_name,displayed_columns in self._displayed_columns_by_struct_name.items():
            assert len(displayed_columns) > 0
            settings[struct_name] = {'auto_colorize_column':None}
            settings[struct_name]['displayed_columns'] = copy.deepcopy(displayed_columns)
        
        for struct_name,auto_colorize_column in self._auto_colorize_column_by_struct_name.items():
            assert struct_name in settings
            settings[struct_name]['auto_colorize_column'] = None if not auto_colorize_column else auto_colorize_column

        return settings
    
    def ApplyViewSettings(self, settings):
        self._displayed_columns_by_struct_name = {}
        self._auto_colorize_column_by_struct_name = {}

        for struct_name,struct_settings in settings.items():
            displayed_columns = struct_settings['displayed_columns']
            auto_colorize_column = struct_settings['auto_colorize_column']

            self._displayed_columns_by_struct_name[struct_name] = copy.deepcopy(displayed_columns)
            self._auto_colorize_column_by_struct_name[struct_name] = auto_colorize_column

        self.frame.inspector.RefreshWidgetsOnAllTabs()

    def GetCurrentUserSettings(self):
        # All our settings are in the user settings and do not affect the view file
        return {}

    def ApplyUserSettings(self, settings):
        # All our settings are in the user settings and do not affect the view file
        pass

    def ResetToDefaultViewSettings(self, update_widgets=True):
        self.cursor.execute('SELECT DISTINCT(StructName) FROM StructFields')
        struct_names = []
        for struct_name in self.cursor.fetchall():
            struct_names.append(struct_name[0])

        self._auto_colorize_column_by_struct_name = {}
        self._displayed_columns_by_struct_name = {}

        for struct_name in struct_names:
            cmd = 'SELECT FieldName FROM StructFields WHERE StructName="{}" AND IsAutoColorizeKey=1'.format(struct_name)
            self.cursor.execute(cmd)
            auto_colorize_column = [row[0] for row in self.cursor.fetchall()]
            assert len(auto_colorize_column) <= 1
            if len(auto_colorize_column) == 1:
                self._auto_colorize_column_by_struct_name[struct_name] = auto_colorize_column[0]
            else:
                self._auto_colorize_column_by_struct_name[struct_name] = None

            cmd = 'SELECT FieldName FROM StructFields WHERE StructName="{}" AND IsDisplayedByDefault=1'.format(struct_name)
            self.cursor.execute(cmd)
            displayed_columns = [row[0] for row in self.cursor.fetchall()]
            assert len(displayed_columns) > 0
            self._displayed_columns_by_struct_name[struct_name] = displayed_columns

        if update_widgets:
            self.frame.inspector.RefreshWidgetsOnAllTabs()

    def SetVisibleFieldNames(self, elem_path, field_names):
        deserializer = self.GetDeserializer(elem_path)
        struct_name = deserializer.struct_name
        assert struct_name in self._displayed_columns_by_struct_name

        if self._displayed_columns_by_struct_name[struct_name] == field_names:
            return

        auto_colorize_col = self.GetAutoColorizeColumn(elem_path)
        if auto_colorize_col not in field_names:
            self.SetAutoColorizeColumn(elem_path, None)

        self._displayed_columns_by_struct_name[struct_name] = copy.deepcopy(field_names)
        self.frame.inspector.RefreshWidgetsOnAllTabs()
        self.frame.view_settings.SetDirty(reason=DirtyReasons.QueueTableDispColsChanged)

    def SetAutoColorizeColumn(self, elem_path, field_name):
        deserializer = self.GetDeserializer(elem_path)
        struct_name = deserializer.struct_name
        assert struct_name in self._auto_colorize_column_by_struct_name

        if self._auto_colorize_column_by_struct_name[struct_name] == field_name:
            return

        self._auto_colorize_column_by_struct_name[struct_name] = field_name
        self.frame.inspector.RefreshWidgetsOnAllTabs()
        self.frame.view_settings.SetDirty(reason=DirtyReasons.QueueTableAutoColorizeChanged)

    def GetAutoColorizeColumn(self, elem_path):
        deserializer = self.GetDeserializer(elem_path)
        struct_name = deserializer.struct_name
        return self._auto_colorize_column_by_struct_name.get(struct_name)

    def GetDeserializer(self, elem_path):
        collection_name = self._collection_names_by_elem_path[elem_path]
        return self._deserializers_by_collection_name[collection_name]
    
    def GetIterableSizesByCollectionID(self, time_val):
        if self._cached_utiliz_time_val is not None and time_val == self._cached_utiliz_time_val:
            return self._cached_utiliz_sizes

        ordered_sizes_by_collection_id = {}
        cmd = 'SELECT DataVals,IsCompressed FROM CollectionData WHERE TimeVal<={} ORDER BY TimeVal DESC LIMIT {}'.format(time_val, self._heartbeat+1)
        self.cursor.execute(cmd)

        records = []
        for record in self.cursor.fetchall():
            records.append(record)

        records.reverse()
        for all_collections_blob,is_compressed in records:
            if is_compressed:
                all_collections_blob = zlib.decompress(all_collections_blob)

            while len(all_collections_blob):
                cid, size = struct.unpack('hh', all_collections_blob[:4])
                all_collections_blob = all_collections_blob[4:]

                collection_name = self._collection_names_by_collection_id[cid]
                deserializer = self._deserializers_by_collection_name[collection_name]
                elem_num_bytes = deserializer.GetNumBytes()

                is_sparse = self._is_sparse_by_collection_id[cid]
                if is_sparse:
                    # This is to handle the extra 2 bytes holding the bucket index.
                    # We only add this to each container element for sparse containers.
                    elem_num_bytes += 2

                if size == 65535:
                    size = -1

                sizes = ordered_sizes_by_collection_id.get(cid, [])
                sizes.append(size)
                ordered_sizes_by_collection_id[cid] = sizes

                if size not in (-1,0):
                    all_collections_blob = all_collections_blob[size*elem_num_bytes:]

        for collection_id,sizes in ordered_sizes_by_collection_id.items():
            for i in range(len(sizes)-1):
                if sizes[i] != -1 and sizes[i+1] == -1:
                    sizes[i+1] = sizes[i]

        sizes_by_collection_id = {}
        for collection_id,sizes in ordered_sizes_by_collection_id.items():
            sizes_by_collection_id[collection_id] = sizes[-1]
        
        self._cached_utiliz_time_val = time_val
        self._cached_utiliz_sizes = sizes_by_collection_id
        return sizes_by_collection_id

    # Get all collected data for the given element by its path. These are the
    # same paths that were used in the original calls to addStat(), addStruct(),
    # and addContainer().
    def Unpack(self, elem_path, time_range=None):
        cursor = self.cursor
        collection_id = self._collection_ids_by_elem_path[elem_path]
        cmd = 'SELECT TimeVal,DataVals,IsCompressed FROM CollectionData '

        if time_range is not None:
            cmd += 'WHERE '
            if type(time_range) in (int,float):
                time_range = (time_range, time_range)

            assert type(time_range) in (list,tuple) and len(time_range) == 2
            where_clauses = []
            if time_range[0] >= 0:
                # Find the first time value that is greater than or equal to the lower bound.
                start_time = None
                for i,time_val in enumerate(self._time_vals):
                    if time_val >= time_range[0]:
                        # Look back a bit to ensure we get all the data, keeping in mind that
                        # for up to self._heartbeat data captures, we may only find the carry-
                        # forward data from the last time we wrote the data to the database.
                        #
                        #    TimeVal     DataVals
                        #    --------    --------
                        #    40          [1,2,3]
                        #    41          carry-forward
                        #    42          [4,5,6]
                        #    43          carry-forward
                        #    44          carry-forward
                        #    45          carry-forward   <-- user wants data from here...
                        #    46          carry-forward
                        #    47          carry-forward
                        #    48          [7,8,9]
                        #    49          carry-forward   <-- ...to here
                        #
                        # If the user wants data from 45-49, we need to read data starting from
                        # time 40. We will end up producing this data in memory:
                        #
                        #    40          [1,2,3]
                        #    41          [1,2,3]        (carry-forward)
                        #    42          [4,5,6]
                        #    43          [4,5,6]        (carry-forward)
                        #    44          [4,5,6]        (carry-forward)
                        #    45          [4,5,6]        (carry-forward)    <-- start here
                        #    46          [4,5,6]        (carry-forward)
                        #    47          [4,5,6]        (carry-forward)
                        #    48          [7,8,9]
                        #    49          [7,8,9]        (carry-forward)    <-- end here
                        #
                        start_idx = i - self._heartbeat
                        start_idx = max(0, start_idx)
                        start_time = self._time_vals[start_idx]
                        break

                where_clauses.append(' TimeVal>={} '.format(start_time))

            if time_range[1] >= 0:
                where_clauses.append(' TimeVal<={} '.format(time_range[1]))

            cmd += ' AND '.join(where_clauses)

        cursor.execute(cmd)

        time_vals = []
        data_vals = []

        collection_name = self._collection_names_by_elem_path[elem_path]
        deserializer = self._deserializers_by_collection_name[collection_name]

        # Get a list of the collection's data blobs in the form:
        # [(time_val, collection_blob), (time_val, collection_blob), ...]
        collection_data_blobs = []

        for time_val, all_collections_blob, is_compressed in cursor.fetchall():
            collection_blob_at_time_val = self._ExtractRawDataBlob(all_collections_blob, is_compressed, collection_id)
            collection_data_blobs.append([self._FormatTimeVal(time_val), collection_blob_at_time_val])

        # Fill in the carry-forward data blobs
        for i in range(0, len(collection_data_blobs)-1):
            if collection_data_blobs[i+1][1] == -1 and collection_data_blobs[i][1] != -1:
                collection_data_blobs[i+1][1] = collection_data_blobs[i][1]

        for time_val, collection_blob in collection_data_blobs:
            if time_range is None or (time_val >= time_range[0] and time_val <= time_range[1]):
                time_vals.append(time_val)
                if collection_blob is not None:
                    data_vals.append(deserializer.Unpack(collection_blob, elem_path))
                else:
                    data_vals.append(None)

        return {'TimeVals': time_vals, 'DataVals': data_vals}

    def GetAllTimeVals(self):
        return copy.deepcopy(self._time_vals)

    def _FormatTimeVal(self, time_val):
        if self._time_type == 'INT':
            return int(time_val)
        elif self._time_type == 'REAL':
            return float(time_val)
        else:
            return time_val
        
    def _ExtractRawDataBlob(self, all_collections_blob, is_compressed, collection_id):
        if is_compressed:
            all_collections_blob = zlib.decompress(all_collections_blob)

        while len(all_collections_blob):
            cid, size = struct.unpack('hh', all_collections_blob[:4])
            all_collections_blob = all_collections_blob[4:]

            collection_name = self._collection_names_by_collection_id[cid]
            deserializer = self._deserializers_by_collection_name[collection_name]
            elem_num_bytes = deserializer.GetNumBytes()

            is_sparse = self._is_sparse_by_collection_id[cid]
            if is_sparse:
                # This is to handle the extra 2 bytes holding the bucket index.
                # We only add this to each container element for sparse containers.
                elem_num_bytes += 2

            if cid == collection_id:
                if size in (65535,-1):
                    return -1
                elif size == 0:
                    return None
                else:
                    return all_collections_blob[:size*elem_num_bytes]
            elif size not in (65535,-1, 0):
                all_collections_blob = all_collections_blob[size*elem_num_bytes:]

class EnumDef:
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
        'double'  :8,
        'bool'    :4
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
        'double'  :'d',
        'bool'    :'i'
    }

    def __init__(self, data_type, element_idxs_by_elem_path, cursor):
        Deserializer.__init__(self, cursor)
        self._scalar_num_bytes = StatsDeserializer.NUM_BYTES_MAP[data_type]
        self._struct_num_bytes = 0
        self._format = StatsDeserializer.FORMAT_CODES_MAP[data_type]
        self._cast = float if data_type in ('float','double') else int
        self._element_idxs_by_elem_path = element_idxs_by_elem_path
        self._isbool = data_type == 'bool'

    def Unpack(self, data_blob, elem_path):
        elem_idx = self._element_idxs_by_elem_path[elem_path]
        start = elem_idx * self._scalar_num_bytes
        end = start + self._scalar_num_bytes
        raw_bytes = data_blob[start:end]
        val = struct.unpack(self._format, raw_bytes)[0]
        val = self._cast(val)
        if self._isbool:
            val = bool(val)

        return val
    
    def GetNumBytes(self):
        return self._scalar_num_bytes

class StructDeserializer(Deserializer):
    def __init__(self, data_retriever, struct_name, strings_by_int, enums_by_name, element_idxs_by_elem_path, cursor):
        Deserializer.__init__(self, cursor)
        self.data_retriever = data_retriever
        self.struct_name = struct_name
        self._strings_by_int = strings_by_int
        self._enums_by_name = enums_by_name
        self._element_idxs_by_elem_path = element_idxs_by_elem_path
        self._field_formatters = []
        self._format = ''
        self._all_field_names = []

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
        self._all_field_names.append(field_name)
        self._struct_num_bytes = 0

    def GetAllFieldNames(self):
        return copy.deepcopy(self._all_field_names)

    def GetVisibleFieldNames(self):
        visible_field_names = self.data_retriever.GetCurrentViewSettings().get(self.struct_name, {}).get('displayed_columns', [])

        field_names = []
        for idx,formatter in enumerate(self._field_formatters):
            if formatter.field_name in visible_field_names:
                field_names.append(formatter.field_name)

        return field_names

    def Unpack(self, data_blob, elem_path, apply_offset=True):
        struct_num_bytes = self.GetStructNumBytes()
        elem_idx = self._element_idxs_by_elem_path[elem_path]

        if apply_offset:
            data_blob = data_blob[struct_num_bytes*elem_idx:struct_num_bytes*(elem_idx+1)]

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

        visible_field_names = self.data_retriever.GetCurrentViewSettings().get(self.struct_name, {}).get('displayed_columns', [])
        assert len(visible_field_names) > 0

        res = {}
        for i,code in enumerate(self._format):
            nbytes = num_bytes_by_format_code[code]
            tiny_blob = data_blob[:nbytes]
            data_blob = data_blob[nbytes:]

            field_name = self._field_formatters[i].field_name
            if field_name not in visible_field_names:
                continue

            val = struct.unpack(code, tiny_blob)[0]
            formatter = self._field_formatters[i]
            res[formatter.field_name] = formatter.Format(val)

        assert len(data_blob) == 0
        return res

    def GetStructNumBytes(self):
        if self._struct_num_bytes > 0:
            return self._struct_num_bytes

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

        self._struct_num_bytes = num_bytes
        return num_bytes
    
    def GetNumBytes(self):
        return self.GetStructNumBytes()

class IterableDeserializer(StructDeserializer):
    def __init__(self, data_retriever, struct_name, strings_by_int, enums_by_name, container_meta_by_elem_path, element_idxs_by_elem_path, cursor):
        StructDeserializer.__init__(self, data_retriever, struct_name, strings_by_int, enums_by_name, element_idxs_by_elem_path, cursor)
        self._container_meta_by_elem_path = container_meta_by_elem_path
        self._element_idxs_by_elem_path = element_idxs_by_elem_path

    def Unpack(self, data_blob, elem_path):
        res = []
        struct_num_bytes = self.GetStructNumBytes()
        sparse = self._container_meta_by_elem_path[elem_path]['IsSparse']

        if not sparse:
            while len(data_blob) > 0:
                struct_blob = data_blob[:struct_num_bytes]
                data_blob = data_blob[struct_num_bytes:]
                res.append(StructDeserializer.Unpack(self, struct_blob, elem_path, False))
        else:
            capacity = self._container_meta_by_elem_path[elem_path]['Capacity']
            res = [None for i in range(capacity)]
            while len(data_blob) > 0:
                bucket_idx = struct.unpack('h', data_blob[:2])[0]
                struct_blob = data_blob[2:2+struct_num_bytes]
                res[bucket_idx] = StructDeserializer.Unpack(self, struct_blob, elem_path, False)
                data_blob = data_blob[2+struct_num_bytes:]

        return res
    
    def GetNumBytes(self):
        return self.GetStructNumBytes()

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
import copy

class SimHierarchy:
    def __init__(self, db):
        child_ids_by_parent_id = {}
        cursor = db.cursor()
        self._root_id = None
        self.__RecurseBuildHierarchy(cursor, 0, child_ids_by_parent_id)
        assert self._root_id is not None

        parent_ids_by_child_id = {}
        for parent_id, child_ids in child_ids_by_parent_id.items():
            for child_id in child_ids:
                parent_ids_by_child_id[child_id] = parent_id

        elem_names_by_id = {}
        self._widget_types_by_elem_id = {}
        cursor.execute("SELECT Id,Name,WidgetType FROM ElementTreeNodes")
        for id,name,widget_type in cursor.fetchall():
            elem_names_by_id[id] = name
            self._widget_types_by_elem_id[id] = widget_type

        def GetLineage(elem_id, parent_ids_by_child_id):
            lineage = []
            while elem_id not in (None,0,self._root_id):
                lineage.append(elem_id)
                elem_id = parent_ids_by_child_id.get(elem_id)

            lineage.reverse()
            return lineage
        
        def GetPath(elem_id, parent_ids_by_child_id, elem_names_by_id):
            lineage = GetLineage(elem_id, parent_ids_by_child_id)
            path = '.'.join([elem_names_by_id.get(elem_id) for elem_id in lineage])
            return path

        elem_paths_by_id = {}
        for elem_id in elem_names_by_id.keys():
            elem_paths_by_id[elem_id] = GetPath(elem_id, parent_ids_by_child_id, elem_names_by_id)

        self._child_ids_by_parent_id = child_ids_by_parent_id
        self._parent_ids_by_child_id = parent_ids_by_child_id
        self._elem_names_by_id = elem_names_by_id
        self._elem_paths_by_id = elem_paths_by_id
        self._elem_ids_by_path = {v: k for k, v in elem_paths_by_id.items()}

        cmd = 'SELECT Id,CollectionID FROM ElementTreeNodes WHERE Id IN ({}) AND CollectionID != -1'.format(','.join(map(str, elem_paths_by_id.keys())))
        cursor.execute(cmd)

        collection_ids_by_elem_path = {}
        for node_id, collection_id in cursor.fetchall():
            elem_path = elem_paths_by_id[node_id]
            collection_ids_by_elem_path[elem_path] = collection_id

        elem_paths_by_collection_id = {}
        for elem_path, collection_id in collection_ids_by_elem_path.items():
            elem_paths = elem_paths_by_collection_id.get(collection_id, [])
            elem_paths.append(elem_path)
            elem_paths_by_collection_id[collection_id] = elem_paths

        cmd = 'SELECT Id,DataType,IsContainer,Capacity FROM Collections'
        cursor.execute(cmd)

        self._scalar_stats_elem_paths = []
        self._scalar_structs_elem_paths = []
        self._container_elem_paths = []

        self._collection_id_by_elem_path = collection_ids_by_elem_path
        self._data_type_by_collection_id = {}
        self._is_container_by_collection_id = {}
        self._capacities_by_collection_id = {}

        for collection_id, data_type, is_container, capacity in cursor.fetchall():
            self._data_type_by_collection_id[collection_id] = data_type
            self._is_container_by_collection_id[collection_id] = is_container
            self._capacities_by_collection_id[collection_id] = capacity

            for elem_path in elem_paths_by_collection_id[collection_id]:
                if data_type in ('int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float', 'double', 'bool'):
                    self._scalar_stats_elem_paths.append(elem_path)
                elif is_container:
                    self._container_elem_paths.append(elem_path)
                else:
                    self._scalar_structs_elem_paths.append(elem_path)

        # Sanity checks to ensure that no element path contains 'root.'
        for _,elem_path in self._elem_paths_by_id.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._elem_ids_by_path.items():
            assert elem_path.find('root.') == -1

        for elem_path,collection_id in collection_ids_by_elem_path.items():
            assert elem_path.find('root.') == -1

        for collection_id,elem_paths in elem_paths_by_collection_id.items():
            for elem_path in elem_paths:
                assert elem_path.find('root.') == -1

        for elem_path in self._scalar_stats_elem_paths:
            assert elem_path.find('root.') == -1

        for elem_path in self._scalar_structs_elem_paths:
            assert elem_path.find('root.') == -1

        for elem_path in self._container_elem_paths:
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._collection_id_by_elem_path.items():
            assert elem_path.find('root.') == -1

    def GetLeafDataType(self, elem_path):
        collection_id = self._collection_id_by_elem_path[elem_path]
        return self._data_type_by_collection_id[collection_id]
    
    def GetRootID(self):
        return self._root_id

    def GetParentID(self, elem_id):
        return self._parent_ids_by_child_id[elem_id]

    def GetChildIDs(self, elem_id):
        return self._child_ids_by_parent_id.get(elem_id, [])
    
    def GetElemPath(self, elem_id):
        return self._elem_paths_by_id[elem_id]
    
    def GetElemID(self, elem_path):
        return self._elem_ids_by_path.get(elem_path)
    
    def GetCollectionID(self, elem_path):
        return self._collection_id_by_elem_path.get(elem_path)
    
    def GetCapacityByCollectionID(self, collection_id):
        return self._capacities_by_collection_id.get(collection_id)
    
    def GetCapacityByElemPath(self, elem_path):
        collection_id = self.GetCollectionID(elem_path)
        capacity = self.GetCapacityByCollectionID(collection_id)
        return capacity
    
    def GetName(self, elem_id):
        return self._elem_names_by_id[elem_id]
    
    def GetElemPaths(self):
        return self._elem_paths_by_id.values()

    def GetScalarStatsElemPaths(self):
        return copy.deepcopy(self._scalar_stats_elem_paths)
    
    def GetScalarStructsElemPaths(self):
        return copy.deepcopy(self._scalar_structs_elem_paths)
    
    def GetContainerElemPaths(self):
        return copy.deepcopy(self._container_elem_paths)

    def GetItemElemPaths(self):
        elem_paths = self.GetScalarStatsElemPaths() + self.GetScalarStructsElemPaths() + self.GetContainerElemPaths()
        elem_paths.sort()
        return elem_paths
    
    def GetWidgetType(self, elem_id):
        return self._widget_types_by_elem_id[elem_id]

    def __RecurseBuildHierarchy(self, cursor, parent_id, child_ids_by_parent_id):
        cursor.execute("SELECT Id FROM ElementTreeNodes WHERE ParentID={}".format(parent_id))
        child_rows = cursor.fetchall()

        for row in child_rows:
            if self._root_id is None and parent_id == 0:
                self._root_id = row[0]
            elif self._root_id is not None and parent_id == 0:
                raise Exception('Multiple roots found in hierarchy')

            child_id = row[0]
            child_ids_by_parent_id[parent_id] = child_ids_by_parent_id.get(parent_id, []) + [child_id]
            self.__RecurseBuildHierarchy(cursor, child_id, child_ids_by_parent_id)

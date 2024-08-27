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
        cursor.execute("SELECT Id,Name FROM ElementTreeNodes")
        for row in cursor.fetchall():
            elem_names_by_id[row[0]] = row[1]

        def GetLineage(elem_id, parent_ids_by_child_id):
            lineage = []
            while elem_id not in (None,0):
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

        cmd = 'SELECT CollectionID,SimPath FROM CollectionElems'
        cursor.execute(cmd)

        collection_ids_by_sim_path = {}
        for collection_id, sim_path in cursor.fetchall():
            collection_ids_by_sim_path[sim_path] = collection_id

        sim_paths_by_collection_id = {}
        for sim_path, collection_id in collection_ids_by_sim_path.items():
            sim_paths = sim_paths_by_collection_id.get(collection_id, [])
            sim_paths.append(sim_path)
            sim_paths_by_collection_id[collection_id] = sim_paths

        cmd = 'SELECT Id,DataType,IsContainer FROM Collections'
        cursor.execute(cmd)

        self._scalar_stats_sim_paths = []
        self._scalar_structs_sim_paths = []
        self._container_sim_paths = []

        for collection_id, data_type, is_container in cursor.fetchall():
            for sim_path in sim_paths_by_collection_id[collection_id]:
                if data_type in ('int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float', 'double'):
                    self._scalar_stats_sim_paths.append(sim_path)
                elif is_container:
                    self._container_sim_paths.append(sim_path)
                else:
                    self._scalar_structs_sim_paths.append(sim_path)

        self._collection_id_by_sim_path = {}
        cursor.execute("SELECT CollectionID,SimPath FROM CollectionElems")
        rows = cursor.fetchall()
        for row in rows:
            self._collection_id_by_sim_path[row[1]] = row[0]

        # Iterate over the Collections table and find the DataType and IsContainer for each CollectionID
        self._data_type_by_collection_id = {}
        self._is_container_by_collection_id = {}
        container_collection_ids = []
        cursor.execute("SELECT Id,DataType,IsContainer FROM Collections")
        rows = cursor.fetchall()
        for row in rows:
            self._data_type_by_collection_id[row[0]] = row[1]
            self._is_container_by_collection_id[row[0]] = row[2]
            if row[2]:
                container_collection_ids.append(row[0])

    def GetLeafDataType(self, sim_path):
        collection_id = self._collection_id_by_sim_path[sim_path]
        return self._data_type_by_collection_id[collection_id]
    
    def GetRootID(self):
        return self._root_id

    def GetParentID(self, db_id):
        return self._parent_ids_by_child_id[db_id]

    def GetChildIDs(self, db_id):
        return self._child_ids_by_parent_id.get(db_id, [])
    
    def GetSimPath(self, db_id):
        return self._elem_paths_by_id[db_id]
    
    def GetName(self, db_id):
        return self._elem_names_by_id[db_id]
    
    def GetSimPaths(self):
        return self._elem_paths_by_id.values()

    def GetScalarStatsSimPaths(self):
        return copy.deepcopy(self._scalar_stats_sim_paths)
    
    def GetScalarStructsSimPaths(self):
        return copy.deepcopy(self._scalar_structs_sim_paths)
    
    def GetContainerSimPaths(self):
        return copy.deepcopy(self._container_sim_paths)

    def GetItemSimPaths(self):
        sim_paths = self.GetScalarStatsSimPaths() + self.GetScalarStructsSimPaths() + self.GetContainerSimPaths()
        sim_paths.sort()
        return sim_paths

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

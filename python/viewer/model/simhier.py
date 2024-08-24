import copy

class SimHierarchy:
    def __init__(self, db, db_ids_by_sim_path):
        self._db_ids_by_sim_path = db_ids_by_sim_path
        self._sim_paths_by_db_id = {v: k for k, v in db_ids_by_sim_path.items()}

        cmd = 'SELECT Id,ParentID,Name FROM ElementTreeNodes'
        db.cursor.execute(cmd)

        self._parent_db_id_map = {}
        self._name_map = {}
        self._root_id = None

        for db_id, parent_db_id, name in db.cursor.fetchall():
            self._parent_db_id_map[db_id] = parent_db_id
            self._name_map[db_id] = name

            if parent_db_id == 0:
                assert self._root_id is None
                self._root_id = db_id

        cmd = 'SELECT CollectionID,SimPath FROM CollectionElems'
        db.cursor.execute(cmd)

        collection_ids_by_sim_path = {}
        for collection_id, sim_path in db.cursor.fetchall():
            collection_ids_by_sim_path[sim_path] = collection_id

        sim_paths_by_collection_id = {}
        for sim_path, collection_id in collection_ids_by_sim_path.items():
            sim_paths = sim_paths_by_collection_id.get(collection_id, [])
            sim_paths.append(sim_path)
            sim_paths_by_collection_id[collection_id] = sim_paths

        cmd = 'SELECT Id,DataType,IsContainer FROM Collections'
        db.cursor.execute(cmd)

        self._scalar_stats_sim_paths = []
        self._scalar_structs_sim_paths = []
        self._container_sim_paths = []

        for collection_id, data_type, is_container in db.cursor.fetchall():
            for sim_path in sim_paths_by_collection_id[collection_id]:
                if data_type in ('int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float', 'double'):
                    self._scalar_stats_sim_paths.append(sim_path)
                elif is_container:
                    self._container_sim_paths.append(sim_path)
                else:
                    self._scalar_structs_sim_paths.append(sim_path)

    def GetRootID(self):
        return self._root_id

    def GetParentID(self, db_id):
        return self._parent_db_id_map.get(db_id)

    def GetChildIDs(self, db_id):
        return [child_db_id for child_db_id, parent_db_id in self._parent_db_id_map.items() if parent_db_id == db_id] 
       
    def GetParentIDBySimPath(self, sim_path):
        db_id = self._db_ids_by_sim_path.get(sim_path)
        return self.GetParentID(db_id)
    
    def GetParentSimPath(self, sim_path):
        parent_db_id = self.GetParentIDBySimPath(sim_path)
        return self.GetSimPathByDbID(parent_db_id)
    
    def GetSimPathByDbID(self, db_id):
        return self._sim_paths_by_db_id.get(db_id)
    
    def GetSimPath(self, db_id):
        return self.GetSimPathByDbID(db_id)
    
    def GetName(self, db_id):
        return self._name_map.get(db_id)
    
    def GetNameBySimPath(self, sim_path):
        db_id = self._db_ids_by_sim_path.get(sim_path)
        return self.GetName(db_id)
    
    def GetSimPaths(self):
        return self._db_ids_by_sim_path.keys()

    def GetScalarStatsSimPaths(self):
        return copy.deepcopy(self._scalar_stats_sim_paths)
    
    def GetScalarStructsSimPaths(self):
        return copy.deepcopy(self._scalar_structs_sim_paths)
    
    def GetContainerSimPaths(self):
        return copy.deepcopy(self._container_sim_paths)

import os, sqlite3

class SimDB:
    def __init__(self, db_path):
        self.db_path = os.path.abspath(os.path.expanduser(db_path))
        self._conn = sqlite3.connect(db_path)
        self._cursor = self._conn.cursor()

    @property
    def cursor(self):
        return self._cursor
    
    @property
    def path(self):
        return self.db_path

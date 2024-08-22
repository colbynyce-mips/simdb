from viewer.model.database import SimDB
from viewer.gui.view_settings import ViewSettings
from viewer.model.frame import ArgosFrame

class Workspace:
    def __init__(self, db_path):
        self._db = SimDB(db_path)
        self._view_settings = ViewSettings()
        self._frame = ArgosFrame(self._view_settings, self._db)
        self._frame.PostLoad()
        self._frame.Show()

    def Cleanup(self):
        self._view_settings.Save()

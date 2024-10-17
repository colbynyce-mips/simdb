from viewer.model.frame import ArgosFrame
from viewer.gui.view_settings import ViewSettings

class Workspace:
    def __init__(self, db_path, views_dir):
        self._view_settings = ViewSettings(views_dir)
        self._frame = ArgosFrame(db_path, self._view_settings)
        self._frame.PostLoad()
        self._frame.Show()

    def Cleanup(self):
        self._view_settings.Save()

import yaml, os

class ViewSettings:
    def __init__(self, views_dir):
        self._view_settings = {}
        self._views_dir = os.path.abspath(views_dir) if views_dir else None

    def Save(self):
        pass

    def GetViewFiles(self):
        if self._views_dir is None:
            return []
        elif not os.path.exists(self._views_dir):
            os.makedirs(self._views_dir)
            return []
        else:
            return [f for f in os.listdir(self._views_dir) if f.endswith('.yaml')]

<<<<<<< Updated upstream
import yaml, os
=======
<<<<<<< Updated upstream
import yaml
>>>>>>> Stashed changes

class ViewSettings:
    def __init__(self, views_dir):
        self._view_settings = {}
        self._views_dir = os.path.abspath(views_dir) if views_dir else None

    def Save(self):
        pass
<<<<<<< Updated upstream
=======
=======
import wx, yaml, os

class ViewSettings:
    def __init__(self, views_dir=None):
        self._views_dir = os.path.abspath(views_dir) if views_dir else os.getcwd()
        self._current_view_file = None
        self._frame = None

    def Save(self, frame):
        settings = {
            'NavTree': frame.explorer.navtree.GetCurrentViewSettings(),
            'Watchlist': frame.explorer.watchlist.GetCurrentViewSettings(),
            'PlaybackBar': frame.playback_bar.GetCurrentViewSettings(),
            'Inspector': frame.inspector.GetCurrentViewSettings()
        }

        if not self.__SettingsChanged(settings):
            return

        # Create a file dialog for saving the file
        with wx.FileDialog(None, "Save Argos View", wildcard="AVF files (*.avf)|*.avf|All files (*.*)|*.*",
                           style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT, defaultDir=self._views_dir) as dlg:
            if dlg.ShowModal() == wx.ID_OK:
                path = dlg.GetPath()
                if not path.endswith('.avf'):
                    path += '.avf'

                with open(path, 'w') as fout:
                    yaml.dump(settings, fout)
                    self._current_view_file = path
>>>>>>> Stashed changes

    def GetViewFiles(self):
        if self._views_dir is None:
            return []
        elif not os.path.exists(self._views_dir):
            os.makedirs(self._views_dir)
            return []
        else:
            return [f for f in os.listdir(self._views_dir) if f.endswith('.yaml')]
<<<<<<< Updated upstream
=======
        
    def PostLoad(self, frame, view_file):
        self._frame = frame
        if view_file:
            self.Load(view_file)
    
    def Load(self, view_file):
        if not os.path.isfile(view_file) and os.path.isdir(self._views_dir):
            view_file = os.path.join(self._views_dir, view_file)

        if not os.path.isfile(view_file):
            msg = f"View file '{view_file}' does not exist in directory '{self._views_dir}'"
            raise RuntimeError(msg)
        
        self._current_view_file = view_file
        with open(view_file, 'r') as fin:
            settings = yaml.load(fin, Loader=yaml.FullLoader)
            self._frame.explorer.navtree.ApplyViewSettings(settings['NavTree'])
            self._frame.explorer.watchlist.ApplyViewSettings(settings['Watchlist'])
            self._frame.playback_bar.ApplyViewSettings(settings['PlaybackBar'])
            self._frame.inspector.ApplyViewSettings(settings['Inspector'])

    def __SettingsChanged(self, new_settings):
        if self._current_view_file is None:
            return True
        
        with open(self._current_view_file, 'r') as fin:
            old_settings = yaml.load(fin, Loader=yaml.FullLoader)
            return old_settings != new_settings
>>>>>>> Stashed changes
>>>>>>> Stashed changes

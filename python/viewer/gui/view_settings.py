import wx, yaml, os

class ViewSettings:
    def __init__(self, views_dir=None):
        self._views_dir = os.path.abspath(views_dir) if views_dir else os.getcwd()
        self._view_file = None
        self._frame = None
        self._dirty = False

    @property
    def dirty(self):
        return self._dirty
    
    @dirty.setter
    def dirty(self, value):
        self._dirty = value
        self.__UpdateTitle()

    @property
    def view_file(self):
        return self._view_file
    
    @view_file.setter
    def view_file(self, value):
        self._view_file = value
        self.__UpdateTitle()

    def GetViewFiles(self):
        if self._views_dir is None:
            return []
        elif not os.path.exists(self._views_dir):
            os.makedirs(self._views_dir)
            return []
        else:
            return [f for f in os.listdir(self._views_dir) if f.endswith('.yaml')]
        
    def PostLoad(self, frame, view_file):
        self._frame = frame
        if view_file:
            self.Load(view_file)

        self.__ApplyUserSettings()
        self.dirty = False
    
    def Load(self, view_file):
        if not os.path.isfile(view_file) and os.path.isdir(self._views_dir):
            view_file = os.path.join(self._views_dir, view_file)

        if not os.path.isfile(view_file):
            msg = f"View file '{view_file}' does not exist in directory '{self._views_dir}'"
            raise RuntimeError(msg)
        
        self.view_file = view_file
        with open(view_file, 'r') as fin:
            settings = yaml.load(fin, Loader=yaml.FullLoader)
            self._frame.explorer.navtree.ApplyViewSettings(settings['NavTree'])
            self._frame.explorer.watchlist.ApplyViewSettings(settings['Watchlist'])
            self._frame.playback_bar.ApplyViewSettings(settings['PlaybackBar'])
            self._frame.data_retriever.ApplyViewSettings(settings['DataRetriever'])
            self._frame.inspector.ApplyViewSettings(settings['Inspector'])

        self._frame.inspector.RefreshWidgetsOnAllTabs()
        self.dirty = False

    def Save(self):
        self.__SaveUserSettings()

        if not self._dirty:
            return

        if self.view_file is None:
            # Ask the user if they want to save the view to a new file
            dlg = wx.MessageDialog(self._frame, "Save current Argos view to new file?", "Save View", wx.YES_NO | wx.CANCEL | wx.ICON_QUESTION)
        else:
            dlg = wx.MessageDialog(self._frame, "Save changes to '{}'?".format(self.view_file), "Save View", wx.YES_NO | wx.CANCEL | wx.ICON_QUESTION)

        result = dlg.ShowModal()
        dlg.Destroy()

        if result == wx.ID_CANCEL:
            return
        
        view_file = self.view_file
        if not view_file:
            with wx.FileDialog(None, "Save Argos View", wildcard="AVF files (*.avf)|*.avf|All files (*.*)|*.*",
                            style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT, defaultDir=self._views_dir) as dlg:
                if dlg.ShowModal() == wx.ID_OK:
                    path = dlg.GetPath()
                    if not path.endswith('.avf'):
                        path += '.avf'

                    view_file = path

        if not view_file:
            return

        settings = {
            'NavTree': self._frame.explorer.navtree.GetCurrentViewSettings(),
            'Watchlist': self._frame.explorer.watchlist.GetCurrentViewSettings(),
            'PlaybackBar': self._frame.playback_bar.GetCurrentViewSettings(),
            'DataRetriever': self._frame.data_retriever.GetCurrentViewSettings(),
            'Inspector': self._frame.inspector.GetCurrentViewSettings()
        }

        with open(view_file, 'w') as fout:
            yaml.dump(settings, fout)

        self.view_file = view_file
        self.dirty = False

    def __UpdateTitle(self):
        if self._frame is None:
            return

        view_file = self.view_file
        dirty = self.dirty

        if view_file is None:
            view_file = 'unnamed'

        title = 'Argos Viewer: %s' % view_file
        if dirty:
            title += '*'

        self._frame.SetTitle(title)

    def __SaveUserSettings(self):
        settings_dir = os.path.expanduser('~/.argos')
        if not os.path.exists(settings_dir):
            os.makedirs(settings_dir)
        
        settings_file = os.path.join(settings_dir, 'user_settings.yaml')

        settings = {
            'NavTree': self._frame.explorer.navtree.GetCurrentUserSettings(),
            'Watchlist': self._frame.explorer.watchlist.GetCurrentUserSettings(),
            'PlaybackBar': self._frame.playback_bar.GetCurrentUserSettings(),
            'DataRetriever': self._frame.data_retriever.GetCurrentUserSettings(),
            'Inspector': self._frame.inspector.GetCurrentUserSettings()
        }

        with open(settings_file, 'w') as fout:
            yaml.dump(settings, fout)

    def __ApplyUserSettings(self):
        settings_dir = os.path.expanduser('~/.argos')
        if not os.path.exists(settings_dir):
            return
        
        settings_file = os.path.join(settings_dir, 'user_settings.yaml')
        if not os.path.exists(settings_file):
            return
        
        with open(settings_file, 'r') as fin:
            settings = yaml.load(fin, Loader=yaml.FullLoader)
            self._frame.explorer.navtree.ApplyUserSettings(settings['NavTree'])
            self._frame.explorer.watchlist.ApplyUserSettings(settings['Watchlist'])
            self._frame.playback_bar.ApplyUserSettings(settings['PlaybackBar'])
            self._frame.data_retriever.ApplyUserSettings(settings['DataRetriever'])
            self._frame.inspector.ApplyUserSettings(settings['Inspector'])

import wx, yaml, os, enum

class DirtyReasons(enum.Enum):
    WidgetDropped = 1
    WidgetSplit = 2
    CanvasExploded = 3
    TabAdded = 4
    TabRenamed = 5
    TabDeleted = 6
    WatchlistAdded = 7
    WatchlistRemoved = 8
    WatchlistOrgChanged = 9
    QueueUtilizDispQueueChanged = 10
    TimeseriesPlotSettingsChanged = 11
    QueueTableDispColsChanged = 12
    QueueTableAutoColorizeChanged = 13
    SashPositionChanged = 14

DIRTY_REASONS = {
    DirtyReasons.WidgetDropped: 'A widget was dropped onto the widget canvas',
    DirtyReasons.WidgetSplit: 'A widget was split horizontally or vertically',
    DirtyReasons.CanvasExploded: 'Widget canvas was exploded',
    DirtyReasons.TabAdded: 'A new tab was added',
    DirtyReasons.TabRenamed: 'A tab was renamed',
    DirtyReasons.TabDeleted: 'A tab was deleted',
    DirtyReasons.WatchlistAdded: 'Something was added to the Watchlist',
    DirtyReasons.WatchlistRemoved: 'Something was removed from the Watchlist',
    DirtyReasons.WatchlistOrgChanged: 'The Watchlist organization (flat/hier) was changed',
    DirtyReasons.QueueUtilizDispQueueChanged: 'The displayed queues in a Queue Utilization widget were changed',
    DirtyReasons.TimeseriesPlotSettingsChanged: 'Settings were changed for a timeseries plot',
    DirtyReasons.QueueTableDispColsChanged: 'Displayed columns were changed for a Queue Table widget',
    DirtyReasons.QueueTableAutoColorizeChanged: 'Auto-colorize column was changed for a Queue Table widget',
    DirtyReasons.SashPositionChanged: 'Widget canvas splitter window sash position changed'
}

class ViewSettings:
    def __init__(self, views_dir=None):
        self._views_dir = os.path.abspath(views_dir) if views_dir else os.getcwd()
        self._view_file = None
        self._frame = None
        self._dirty = False
        self._dirty_reasons = set()

    @property
    def view_file(self):
        return self._view_file
    
    @view_file.setter
    def view_file(self, value):
        self._view_file = value
        self.__UpdateTitle()

    @property
    def dirty(self):
        return self._dirty

    def SetDirty(self, dirty=True, reason=None):
        self._dirty = dirty
        if dirty and reason is not None:
            assert isinstance(reason, DirtyReasons)
            self._dirty_reasons.add(reason)
        elif not dirty:
            self._dirty_reasons.clear()

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
        self.SetDirty(False)
    
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
            self._frame.widget_renderer.ApplyViewSettings(settings['WidgetRenderer'])

        self._frame.inspector.RefreshWidgetsOnAllTabs()
        self.SetDirty(False)

    # Note that this method returns True if Argos can be closed after calling this method.
    def Save(self):
        self.__SaveUserSettings()

        if not self._dirty:
            return True

        if self.view_file is None:
            # Ask the user if they want to save the view to a new file
            dlg = SaveViewFileDlg(prompt="Save current Argos view to a new file?", reasons=self._dirty_reasons)
        else:
            dlg = SaveViewFileDlg(prompt="Save changes to '{}'?".format(self.view_file), reasons=self._dirty_reasons)

        result = dlg.ShowModal()
        dlg.Destroy()
        if result == wx.ID_CANCEL:
            return False

        if result == wx.ID_YES:
            view_file = self.view_file
            if not view_file:
                with wx.FileDialog(None, "Save Argos View", wildcard="AVF files (*.avf)|*.avf|All files (*.*)|*.*",
                                style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT, defaultDir=self._views_dir) as dlg:
                    if dlg.ShowModal() == wx.ID_OK:
                        path = dlg.GetPath()
                        if not path.endswith('.avf'):
                            path += '.avf'

                        view_file = path

            # Do not close Argos if the user cancels the save dialog
            if not view_file:
                return False
            
        if result == wx.ID_NO:
            return True

        settings = {
            'NavTree': self._frame.explorer.navtree.GetCurrentViewSettings(),
            'Watchlist': self._frame.explorer.watchlist.GetCurrentViewSettings(),
            'PlaybackBar': self._frame.playback_bar.GetCurrentViewSettings(),
            'DataRetriever': self._frame.data_retriever.GetCurrentViewSettings(),
            'Inspector': self._frame.inspector.GetCurrentViewSettings(),
            'WidgetRenderer': self._frame.widget_renderer.GetCurrentViewSettings()
        }

        with open(view_file, 'w') as fout:
            yaml.dump(settings, fout)

        self.view_file = view_file
        self.SetDirty(False)
        return True

    def __UpdateTitle(self):
        if self._frame is None:
            return

        view_file = self.view_file
        dirty = self._dirty

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
            'Inspector': self._frame.inspector.GetCurrentUserSettings(),
            'WidgetRenderer': self._frame.widget_renderer.GetCurrentUserSettings()
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
            self._frame.widget_renderer.ApplyUserSettings(settings['WidgetRenderer'])

class SaveViewFileDlg(wx.Dialog):
    def __init__(self, prompt='Save to view file?', reasons=None):
        super().__init__(None, title='Save View', size=(550, 200))

        self._reasons = reasons
        panel = wx.Panel(self)
        sizer = wx.BoxSizer(wx.VERTICAL)

        instruction = wx.StaticText(panel, label=prompt)
        sizer.Add(instruction, 0, wx.ALL | wx.CENTER, 10)

        btn_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Save button
        save_btn = wx.Button(panel, label="Save")
        save_btn.Bind(wx.EVT_BUTTON, lambda event: self.EndModal(wx.ID_YES))
        btn_sizer.Add(save_btn, 0, wx.ALL | wx.CENTER, 5)

        # Do not save button (closes Argos)
        do_not_save_btn = wx.Button(panel, label="Do not save")
        do_not_save_btn.Bind(wx.EVT_BUTTON, lambda event: self.EndModal(wx.ID_NO))
        btn_sizer.Add(do_not_save_btn, 0, wx.ALL | wx.CENTER, 5)

        # Cancel button
        cancel_btn = wx.Button(panel, label="Cancel")
        cancel_btn.Bind(wx.EVT_BUTTON, lambda event: self.EndModal(wx.ID_CANCEL))
        btn_sizer.Add(cancel_btn, 0, wx.ALL | wx.CENTER, 5)

        # "What changed?" button
        what_changed_btn = wx.Button(panel, wx.ID_ANY, label="What changed?")
        what_changed_btn.Bind(wx.EVT_BUTTON, self.__ShowWhatChanged)
        btn_sizer.Add(what_changed_btn, 0, wx.ALL | wx.CENTER, 5)

        sizer.Add(btn_sizer, 0, wx.ALL | wx.RIGHT, 10)
        panel.SetSizer(sizer)

    def __ShowWhatChanged(self, event):
        if self._reasons is None:
            return

        if len(self._reasons) > 1:
            msg = 'The following changes were made to the view:\n\n'
            for i,reason in enumerate(self._reasons):
                msg += str(i) + '.  ' + DIRTY_REASONS[reason] + '\n'
        else:
            msg = DIRTY_REASONS[self._reasons.pop()]

        dlg = wx.MessageDialog(self, msg, 'Changes', wx.OK | wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()

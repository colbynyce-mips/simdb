import wx, os

class Views(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(Views, self).__init__(parent, style=wx.TR_DEFAULT_STYLE | wx.TR_LINES_AT_ROOT)
        self.frame = frame

        self._root = self.AddRoot("Views")
        view_files = frame.view_settings.GetViewFiles()
        for view_file in view_files:
            view_name = os.path.basename(view_file)
            self.AppendItem(self._root, view_name)

        self.ExpandAll()

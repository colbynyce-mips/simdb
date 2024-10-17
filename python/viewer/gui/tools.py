import wx

class SystemwideTools(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(SystemwideTools, self).__init__(parent, style=wx.TR_DEFAULT_STYLE | wx.TR_HIDE_ROOT | wx.TR_LINES_AT_ROOT)
        self.frame = frame

        self._root = self.AddRoot("root")
        tools_node = self.AppendItem(self._root, "Systemwide Tools")
        self.AppendItem(tools_node, "Queue Utilization")
        self.AppendItem(tools_node, "Packet Tracker")
        self.AppendItem(tools_node, "Scheduling Lines")
        self.AppendItem(tools_node, "Timeseries Viewer")

        self.ExpandAll()

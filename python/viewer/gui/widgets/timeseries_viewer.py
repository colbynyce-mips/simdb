import wx
    
class TimeseriesViewerWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Timeseries Viewer'), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'Timeseries Viewer'

    def UpdateWidgetData(self):
        pass
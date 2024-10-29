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

    def GetErrorIfDroppedNodeIncompatible(self, elem_path):
        return None

    def AddElement(self, elem_path):
        pass

    def UpdateWidgetData(self):
        pass

    def GetCurrentViewSettings(self):
        return {}
    
    def GetCurrentUserSettings(self):
        return {}

    def ApplyViewSettings(self, settings):
        pass

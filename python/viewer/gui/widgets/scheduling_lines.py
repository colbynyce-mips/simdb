import wx
    
class SchedulingLinesWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Scheduling Lines'), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'Scheduling Lines'

    def UpdateWidgetData(self):
        pass

    def GetCurrentViewSettings(self):
        return {}
    
    def GetCurrentUserSettings(self):
        return {}

    def ApplyViewSettings(self, settings):
        pass

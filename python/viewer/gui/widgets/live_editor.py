import wx
    
class LiveEditorWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Live Editor'), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'Live Editor'

    def UpdateWidgetData(self):
        pass
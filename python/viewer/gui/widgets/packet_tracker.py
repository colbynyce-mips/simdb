import wx
    
class PacketTrackerWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Packet Tracker'), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'Packet Tracker'

    def UpdateWidgetData(self):
        pass
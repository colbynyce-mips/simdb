import wx
from viewer.gui.widgets.toolbase import ToolBase

class PacketTrackerTool(ToolBase):
    def __init__(self):
        super().__init__()

    def GetToolName(self):
        return "Packet Tracker"
    
    def GetToolSettings(self):
        return None
    
    def SetToolSettings(self, settings):
        pass

    def CreateWidget(self, parent, frame):
        return PacketTrackerWidget(parent, frame)
    
    def GetToolHelpText(self):
        return "TBD"
    
    def ShowHelpDialog(self, help_text):
        wx.MessageBox(help_text, "Packet Tracker Help", wx.OK | wx.ICON_INFORMATION)
    
class PacketTrackerWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Packet Tracker'), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def UpdateWidgetData(self):
        pass
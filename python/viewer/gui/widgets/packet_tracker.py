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

    def CreateToolWidget(self, parent, frame):
        return PacketTrackerWidget(parent, frame)
    
    def GetToolHelpText(self):
        return "TBD"
    
    def ShowHelpDialog(self, help_text):
        wx.MessageBox(help_text, "Packet Tracker Help", wx.OK | wx.ICON_INFORMATION)
    
class PacketTrackerWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.SetBackgroundColour('green')
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(self.sizer)
        self.Layout()

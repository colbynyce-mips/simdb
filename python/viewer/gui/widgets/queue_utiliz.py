import wx
from viewer.gui.widgets.toolbase import ToolBase

class QueueUtilizTool(ToolBase):
    def __init__(self):
        super().__init__()

    def GetToolName(self):
        return "Queue Utilization"
    
    def GetToolSettings(self):
        return None
    
    def SetToolSettings(self, settings):
        pass

    def CreateToolWidget(self, parent, frame):
        return QueueUtilizWidget(parent, frame)
    
    def GetToolHelpText(self):
        return "Use this tool to see how full all queues \n" \
               "are at once using a heatmap. This lets you\n" \
               "easily spot system backpressure."
    
    def ShowHelpDialog(self, help_text):
        wx.MessageBox(help_text, "Queue Utilization Help", wx.OK | wx.ICON_INFORMATION)
    
class QueueUtilizWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.SetBackgroundColour('red')
        self.frame = frame

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(self.sizer)
        self.Layout()

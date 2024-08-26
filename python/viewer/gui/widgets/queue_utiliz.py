import wx, random, math
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

    def CreateWidget(self, parent, frame):
        return QueueUtilizWidget(parent, frame)
    
    def GetToolHelpText(self):
        return "Use this tool to see how full all queues \n" \
               "are at once using a heatmap. This lets you\n" \
               "easily spot system backpressure."
    
    def ShowHelpDialog(self, help_text):
        wx.MessageBox(help_text, "Queue Utilization Help", wx.OK | wx.ICON_INFORMATION)
    
class QueueUtilizWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent, size=(800, 500))
        self.frame = frame

        # Get all container sim paths from the simhier
        self.container_sim_paths = self.frame.simhier.GetContainerSimPaths()
        self.container_sim_paths.sort()

        # The layout of this widget is like a barchart:
        #
        # sim.path.foo    [28%  XXXXXXXXXX                           ]
        # sim.path.bar    [15%  XXXX                                 ]
        # sim.path.fizz   [19%  XXXXXX                               ]
        # sim.path.buzz   [3%   X                                    ]
        # sim.path.fizbuz [15%  XXXXX                                ]
        #
        # Where the X's above are shown as a colored heatmap based on the
        # utilization percentage of each queue.
        self._sim_path_text_elems = [wx.StaticText(self, label=sim_path) for sim_path in self.container_sim_paths]
        self._utiliz_bars = [UtilizBar(self, frame) for _ in range(len(self.container_sim_paths))]

        # Change the font to 8-point monospace.
        font = wx.Font(8, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)
        for elem in self._sim_path_text_elems:
            elem.SetFont(font)

        # Layout the widgets
        sizer = wx.FlexGridSizer(2, 0, 10)
        sizer.AddGrowableCol(1)
        for sim_path, utiliz_bar in zip(self._sim_path_text_elems, self._utiliz_bars):
            sizer.Add(sim_path, 1, wx.EXPAND)
            sizer.Add(utiliz_bar)

        self.SetSizer(sizer)
        self.Layout()

    def UpdateWidgetData(self):
        for sim_path, pct_bar in zip(self.container_sim_paths, self._utiliz_bars):
            utiliz_pct = self.frame.widget_renderer.utiliz_handler.GetUtilizPct(sim_path)
            pct_bar.UpdateUtilizPct(utiliz_pct)

class UtilizBar(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame
        self.static_text = wx.StaticText(self, label='0%')
        font = wx.Font(8, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)
        self.static_text.SetFont(font)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.static_text, 1, wx.ALIGN_LEFT | wx.EXPAND)
        self.SetSizer(sizer)

    def UpdateUtilizPct(self, utiliz_pct):
        self.static_text.SetLabel('{}%'.format(round(utiliz_pct * 100)))
        color = self.frame.widget_renderer.utiliz_handler.ConvertUtilizPctToColor(utiliz_pct)
        self.SetBackgroundColour(color)
        self.static_text.SetBackgroundColour(color)
        
        height = self.GetSize().GetHeight()
        width = round(utiliz_pct * 500)
        self.SetSize((width, height))

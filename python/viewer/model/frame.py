import wx
from viewer.gui.widgets.playback_bar import PlaybackBar
from viewer.gui.explorer import DataExplorer
from viewer.gui.inspector import DataInspector
from viewer.gui.widgets.queue_utiliz import QueueUtilizTool
from viewer.gui.widgets.packet_tracker import PacketTrackerTool
from viewer.gui.widgets.live_editor import LiveEditorTool

class ArgosFrame(wx.Frame):
    def __init__(self, view_settings, db):
        super().__init__(None, title=db.path)
        
        self.view_settings = view_settings
        self.db = db

        frame_splitter = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
        self.explorer = DataExplorer(frame_splitter, self)
        self.inspector = DataInspector(frame_splitter, self)
        self.playback_bar = PlaybackBar(self)

        frame_splitter.SplitVertically(self.explorer, self.inspector, sashPosition=300)
        frame_splitter.SetMinimumPaneSize(300)

        # Layout
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(frame_splitter, 1, wx.EXPAND)
        sizer.Add(self.playback_bar, 0, wx.EXPAND)
        self.SetSizer(sizer)
        self.Layout()
        self.Maximize()

    def PostLoad(self):
        self.explorer.navtree.AddTool(QueueUtilizTool())
        self.explorer.navtree.AddTool(PacketTrackerTool())
        self.explorer.navtree.AddTool(LiveEditorTool())

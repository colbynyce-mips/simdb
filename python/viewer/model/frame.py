import wx, sqlite3
from viewer.gui.widgets.playback_bar import PlaybackBar
from viewer.gui.explorer import DataExplorer
from viewer.gui.inspector import DataInspector
from viewer.gui.widgets.queue_utiliz import QueueUtilizTool
from viewer.gui.widgets.packet_tracker import PacketTrackerTool
from viewer.gui.widgets.live_editor import LiveEditorTool
from viewer.gui.widgets.scalar_statistic import ScalarStatistic
from viewer.gui.widgets.scalar_struct import ScalarStruct
from viewer.gui.widgets.iterable_struct import IterableStruct
from viewer.gui.widgets.widget_renderer import WidgetRenderer
from viewer.model.data_retriever import DataRetriever
from viewer.gui.view_settings import ViewSettings

class ArgosFrame(wx.Frame):
    def __init__(self, db_path, view_settings):
        super().__init__(None, title=db_path)
        
        self.db = sqlite3.connect(db_path)
        self.view_settings = view_settings
        self.widget_renderer = WidgetRenderer(self)
        self.data_retriever = DataRetriever(self.db)

        self.frame_splitter = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
        self.explorer = DataExplorer(self.frame_splitter, self)
        self.inspector = DataInspector(self.frame_splitter, self)
        self.playback_bar = PlaybackBar(self)

        self.frame_splitter.SplitVertically(self.explorer, self.inspector, sashPosition=300)
        self.frame_splitter.SetMinimumPaneSize(300)

        # Layout
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.frame_splitter, 1, wx.EXPAND)
        sizer.Add(self.playback_bar, 0, wx.EXPAND)
        self.SetSizer(sizer)
        self.Layout()
        self.Maximize()

    @property
    def simhier(self):
        return self.explorer.navtree.simhier

    def PostLoad(self):
        self.explorer.navtree.AddSystemWideTool(QueueUtilizTool())
        self.explorer.navtree.AddSystemWideTool(PacketTrackerTool())
        self.explorer.navtree.AddSystemWideTool(LiveEditorTool())

        self.widget_renderer.SetWidgetFactory('ScalarStatistic', ScalarStatistic.CreateWidget)
        self.widget_renderer.SetWidgetFactory('ScalarStruct', ScalarStruct.CreateWidget)
        self.widget_renderer.SetWidgetFactory('IterableStruct', IterableStruct.CreateWidget)

        self.explorer.navtree.PostLoad()

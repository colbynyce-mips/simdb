import wx, sqlite3
from viewer.gui.widgets.playback_bar import PlaybackBar
from viewer.gui.explorer import DataExplorer
from viewer.gui.inspector import DataInspector
from viewer.gui.widgets.widget_renderer import WidgetRenderer
from viewer.gui.widgets.widget_creator import WidgetCreator
from viewer.model.data_retriever import DataRetriever
from viewer.model.simhier import SimHierarchy

class ArgosFrame(wx.Frame):
    def __init__(self, db_path, view_settings):
        super().__init__(None, title=db_path)
        
        self.db = sqlite3.connect(db_path)
        self.view_settings = view_settings
        self.simhier = SimHierarchy(self.db)
        self.widget_renderer = WidgetRenderer(self)
        self.widget_creator = WidgetCreator(self)
        self.data_retriever = DataRetriever(self.db, self.simhier)

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

    def PostLoad(self, view_file):
        self.widget_creator.BindToWidgetSource(self.explorer.navtree)
        self.widget_creator.BindToWidgetSource(self.explorer.watchlist)
        self.widget_creator.BindToWidgetSource(self.explorer.tools)
        self.view_settings.PostLoad(self, view_file)

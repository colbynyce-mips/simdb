import wx, sqlite3, os
from viewer.gui.widgets.playback_bar import PlaybackBar
from viewer.gui.explorer import DataExplorer
from viewer.gui.inspector import DataInspector
from viewer.gui.widgets.widget_renderer import WidgetRenderer
from viewer.gui.widgets.widget_creator import WidgetCreator
from viewer.model.data_retriever import DataRetriever
from viewer.model.simhier import SimHierarchy
from viewer.gui.widgets.splitter_window import DirtySplitterWindow

class ArgosFrame(wx.Frame):
    def __init__(self, db_path, view_settings):
        super().__init__(None, title=db_path)
        
        self.db = sqlite3.connect(db_path)
        self.view_settings = view_settings
        self.simhier = SimHierarchy(self.db)
        self.widget_renderer = WidgetRenderer(self)
        self.widget_creator = WidgetCreator(self)
        self.data_retriever = DataRetriever(self, self.db, self.simhier)

        self.frame_splitter = DirtySplitterWindow(self, self, style=wx.SP_LIVE_UPDATE)
        self.explorer = DataExplorer(self.frame_splitter, self)
        self.inspector = DataInspector(self.frame_splitter, self)
        self.playback_bar = PlaybackBar(self)

        self.frame_splitter.SplitVertically(self.explorer, self.inspector, sashPosition=300)
        self.frame_splitter.SetMinimumPaneSize(300)

        self.menu_bar = wx.MenuBar()
        file_menu = wx.Menu()

        new_view = file_menu.Append(wx.ID_NEW, '&New View\tCtrl+N', 'Create a new view')
        open_view = file_menu.Append(wx.ID_OPEN, '&Open View\tCtrl+O', 'Open an existing view')
        save_view = file_menu.Append(wx.ID_SAVE, '&Save View\tCtrl+S', 'Save the current view')
        save_view_as = file_menu.Append(wx.ID_SAVEAS, 'Save View &As...\tCtrl+Shift+S', 'Save the current view as a new .avf file')

        self.Bind(wx.EVT_MENU, self.__OnNewView, new_view)
        self.Bind(wx.EVT_MENU, self.__OnOpenView, open_view)
        self.Bind(wx.EVT_MENU, self.__OnSaveView, save_view)
        self.Bind(wx.EVT_MENU, self.__OnSaveViewAs, save_view_as)

        self.menu_bar.Append(file_menu, '&File')
        self.SetMenuBar(self.menu_bar)

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

    def CreateResourceBitmap(self, filename, size=(16, 16)):
        w,h = size
        resources_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'resources'))
        filepath = os.path.join(resources_dir, filename)
        bitmap = wx.Bitmap(filepath, wx.BITMAP_TYPE_PNG)
        bitmap = bitmap.ConvertToImage().Rescale(w,h).ConvertToBitmap()
        return bitmap

    def __OnNewView(self, event):
        self.view_settings.CreateNewView()

    def __OnOpenView(self, event):
        self.view_settings.OpenView()

    def __OnSaveView(self, event):
        self.view_settings.SaveView()

    def __OnSaveViewAs(self, event):
        self.view_settings.SaveViewAs()

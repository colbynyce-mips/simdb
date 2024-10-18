import wx
from viewer.gui.navtree import NavTree
from viewer.gui.watchlist import Watchlist
from viewer.gui.tools import SystemwideTools
from viewer.gui.views import Views

class DataExplorer(wx.Notebook):
    def __init__(self, parent, frame):
        super(DataExplorer, self).__init__(parent, style=wx.NB_LEFT)
        self.frame = frame
        self.navtree = NavTree(self, frame)
        self.watchlist = Watchlist(self, frame)
        self.tools = SystemwideTools(self, frame)
        self.views = Views(self, frame)

        self.AddPage(self.navtree, "NavTree")
        self.AddPage(self.watchlist, "Watchlist")
        self.AddPage(self.tools, "Tools")
        self.AddPage(self.views, "Views")

        self.SetMinSize((200, 200))

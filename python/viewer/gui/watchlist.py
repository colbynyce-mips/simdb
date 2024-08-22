import wx
from functools import partial

class Watchlist(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(Watchlist, self).__init__(parent)
        self.frame = frame
        self._watched_sim_elems = []

        sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(sizer)
        self.__RenderFlatView()

        self.Bind(wx.EVT_TREE_ITEM_GETTOOLTIP, self.__ProcessTooltip)
        self.Bind(wx.EVT_RIGHT_DOWN, self.__OnRightClick)

    def GetWatchedSimElems(self):
        return self._watched_sim_elems
    
    def AddToWatchlist(self, elem_path):
        self._watched_sim_elems.append(elem_path)
        self.__RenderFlatView()
        self.GetParent().ChangeSelection(1)

    def __RenderFlatView(self, *args, **kwargs):
        self.DeleteAllItems()
        if len(self._watched_sim_elems) > 0:
            self.AddRoot("Watchlist")

        sizer = self.GetSizer()
        sizer.Clear(True)

        if len(self._watched_sim_elems) == 0:
            howto = wx.StaticText(self, label="Right-click nodes in the \nNavTree to add them to \nthe Watchlist.",
                                  style=wx.ST_ELLIPSIZE_END)
            
            # Make the font size of howto smaller
            font = howto.GetFont()
            font.SetPointSize(font.GetPointSize() - 2)
            howto.SetFont(font)

            sizer.Add(howto, 1, wx.EXPAND | wx.ALL, 10)
        else:
            for elem in self._watched_sim_elems:
                self.AppendItem(self.GetRootItem(), elem)

            self.ExpandAll()

    def __RenderHierView(self, *args, **kwargs):
        self.DeleteAllItems()
        if len(self._watched_sim_elems) > 0:
            self.AddRoot("Watchlist")

        sizer = self.GetSizer()
        sizer.Clear(True)

        if len(self._watched_sim_elems) == 0:
            howto = wx.StaticText(self, label="Right-click nodes in the \nNavTree to add them to \nthe Watchlist.",
                                  style=wx.ST_ELLIPSIZE_END)
            
            # Make the font size of howto smaller
            font = howto.GetFont()
            font.SetPointSize(font.GetPointSize() - 2)
            howto.SetFont(font)

            sizer.Add(howto, 1, wx.EXPAND | wx.ALL, 10)
        else:
            items_by_path = {}
            items_by_path["Root"] = self.GetRootItem()

            # Honor the same hierarchy as the NavTree
            navtree_leaf_paths = self.frame.explorer.navtree.GetLeafElementPaths()

            for path in navtree_leaf_paths:
                if path not in self._watched_sim_elems:
                    continue

                parts = path.split('.')
                current_item = self.GetRootItem()
                
                for part in parts:
                    item_key = ".".join(parts[:parts.index(part)+1])
                    
                    # Check if the item already exists
                    if item_key not in items_by_path:
                        current_item = self.AppendItem(current_item, part)
                        items_by_path[item_key] = current_item
                    else:
                        current_item = items_by_path[item_key]

            self.ExpandAll()

    def __ProcessTooltip(self, event):
        item = event.GetItem()
        event.SetToolTip(self.GetItemText(item))
        event.Skip()

    def __OnRightClick(self, event):
        item = self.HitTest(event.GetPosition())
        if not item:
            return

        item = item[0]
        self.SelectItem(item)

        menu = wx.Menu()

        def ExpandAll(event, **kwargs):
            kwargs['watchlist'].ExpandAll()
            event.Skip()

        def CollapseAll(event, **kwargs):
            kwargs['watchlist'].CollapseAll()
            event.Skip()

        expand_all = menu.Append(-1, "Expand All")
        self.Bind(wx.EVT_MENU, partial(ExpandAll, watchlist=self), expand_all)

        collapse_all = menu.Append(-1, "Collapse All")
        self.Bind(wx.EVT_MENU, partial(CollapseAll, watchlist=self), collapse_all)

        menu.AppendSeparator()

        flat_view = menu.Append(-1, "Flat View")
        self.Bind(wx.EVT_MENU, self.__RenderFlatView, flat_view)

        hier_view = menu.Append(-1, "Hierarchical View")
        self.Bind(wx.EVT_MENU, self.__RenderHierView, hier_view)

        menu.AppendSeparator()

        remove_from_watchlist = menu.Append(-1, "Remove from Watchlist")
        self.Bind(wx.EVT_MENU, partial(self.__RemoveFromWatchlist, elem_path=self.GetItemText(item)), remove_from_watchlist)

        self.PopupMenu(menu)
        menu.Destroy()

        event.Skip()

    def __RemoveFromWatchlist(self, *args, **kwargs):
        elem_path = kwargs['elem_path']
        self._watched_sim_elems.remove(elem_path)
        self.__RenderFlatView()

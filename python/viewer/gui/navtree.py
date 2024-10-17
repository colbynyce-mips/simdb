import wx, os
from functools import partial
from viewer.model.simhier import SimHierarchy

class NavTree(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(NavTree, self).__init__(parent, style=wx.TR_DEFAULT_STYLE | wx.TR_HIDE_ROOT | wx.TR_LINES_AT_ROOT)
        self.frame = frame
        self.simhier = SimHierarchy(frame.db)
        cursor = frame.db.cursor()

        self._root = self.AddRoot("root")
        self._tree_items_by_db_id = {self.simhier.GetRootID(): self._root }
        self.__RecurseBuildTree(self.simhier.GetRootID())

        self._container_elem_paths = self.simhier.GetContainerElemPaths()
        self._leaf_element_paths_by_tree_item = {}
        for db_id, tree_item in self._tree_items_by_db_id.items():
            if not self.GetChildrenCount(tree_item):
                self._leaf_element_paths_by_tree_item[tree_item] = self.simhier.GetElemPath(db_id).replace('root.','')

        self._tree_items_by_elem_path = {v: k for k, v in self._leaf_element_paths_by_tree_item.items()}

        self.Bind(wx.EVT_RIGHT_DOWN, self.__OnRightClick)
        self.Bind(wx.EVT_TREE_ITEM_EXPANDED, self.__OnItemExpanded)

        self.__utiliz_image_list = frame.widget_renderer.utiliz_handler.CreateUtilizImageList()
        self.SetImageList(self.__utiliz_image_list)

    def UpdateUtilizBitmaps(self):
        for elem_path in self._container_elem_paths:
            utiliz_pct = self.frame.widget_renderer.utiliz_handler.GetUtilizPct(elem_path)
            image_idx = int(utiliz_pct * 100)
            item = self._tree_items_by_elem_path[elem_path]
            self.SetItemImage(item, image_idx)

    def ExpandAll(self):
        self.Unbind(wx.EVT_TREE_ITEM_EXPANDED)
        super(NavTree, self).ExpandAll()
        self.UpdateUtilizBitmaps()
        self.Bind(wx.EVT_TREE_ITEM_EXPANDED, self.__OnItemExpanded)

    def GetItemElemPath(self, item):
        return self._leaf_element_paths_by_tree_item.get(item, None)

    def __RecurseBuildTree(self, parent_id):
        for child_id in self.simhier.GetChildIDs(parent_id):
            child_name = self.simhier.GetName(child_id)
            child = self.AppendItem(self._tree_items_by_db_id[parent_id], child_name)
            self._tree_items_by_db_id[child_id] = child
            self.__RecurseBuildTree(child_id)

    def __OnRightClick(self, event):
        item = self.HitTest(event.GetPosition())
        if not item:
            return
        
        item = item[0]
        self.SelectItem(item)

        menu = wx.Menu()

        def ExpandAll(event, **kwargs):
            kwargs['navtree'].ExpandAll()
            event.Skip()

        def CollapseAll(event, **kwargs):
            kwargs['navtree'].CollapseAll()
            event.Skip()

        expand_all = menu.Append(-1, "Expand All")
        self.Bind(wx.EVT_MENU, partial(ExpandAll, navtree=self), expand_all)

        collapse_all = menu.Append(-1, "Collapse All")
        self.Bind(wx.EVT_MENU, partial(CollapseAll, navtree=self), collapse_all)

        def AddToWatchlist(*args, **kwargs):
            navtree = kwargs['navtree']
            elem_path = kwargs['elem_path']
            navtree.frame.explorer.watchlist.AddToWatchlist(elem_path)

        def ShowWatchlistHelp(*args, **kwargs):
            wx.MessageBox("Right-click nodes in the NavTree to add them to the Watchlist.\n" \
                          "You will only see 'Add to Watchlist' on the right-click menu if the node \n" \
                          "has a widget associated with it and is not already in the Watchlist.", "Watchlist Help")

        menu.AppendSeparator()

        if item in self._leaf_element_paths_by_tree_item:
            elem_path = self._leaf_element_paths_by_tree_item[item]
            if elem_path not in self.frame.explorer.watchlist.GetWatchedSimElems():
                add_to_watchlist = menu.Append(-1, "Add to Watchlist")
                self.Bind(wx.EVT_MENU, partial(AddToWatchlist, navtree=self, elem_path=elem_path), add_to_watchlist)
            else:
                show_watchlist_help = menu.Append(-1, "Watchlist Help")
                self.Bind(wx.EVT_MENU, ShowWatchlistHelp, show_watchlist_help)
        else:
            show_watchlist_help = menu.Append(-1, "Watchlist Help")
            self.Bind(wx.EVT_MENU, ShowWatchlistHelp, show_watchlist_help)

        self.PopupMenu(menu)
        menu.Destroy()

    def __OnItemExpanded(self, event):
        self.UpdateUtilizBitmaps()

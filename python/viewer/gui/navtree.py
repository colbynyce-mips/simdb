import wx, os
from functools import partial

class NavTree(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(NavTree, self).__init__(parent, style=wx.TR_DEFAULT_STYLE | wx.TR_HIDE_ROOT | wx.TR_LINES_AT_ROOT)
        self.frame = frame
        self.simhier = frame.simhier

        self._root = self.AddRoot("root")
<<<<<<< Updated upstream
        self._tree_items_by_db_id = {self.simhier.GetRootID(): self._root }
=======
<<<<<<< Updated upstream
        self._sim_hier_root = self.AppendItem(self._root, "Sim Hierarchy")
        self._tree_items_by_db_id = {self.simhier.GetRootID(): self._sim_hier_root }
>>>>>>> Stashed changes
        self.__RecurseBuildTree(self.simhier.GetRootID())

        self._leaf_element_paths_by_tree_item = {}
        for db_id, tree_item in self._tree_items_by_db_id.items():
            if not self.GetChildrenCount(tree_item):
                self._leaf_element_paths_by_tree_item[tree_item] = self.simhier.GetElemPath(db_id).replace('root.','')

<<<<<<< Updated upstream
        self._tree_items_by_elem_path = {v: k for k, v in self._leaf_element_paths_by_tree_item.items()}
=======
        self._tree_items_by_sim_path = {v: k for k, v in self._leaf_element_paths_by_tree_item.items()}
=======
        self._tree_items_by_db_id = {self.simhier.GetRootID(): self._root }
        self._tree_items_by_elem_path = {}
        self.__RecurseBuildTree(self.simhier.GetRootID())

        self._leaf_elem_paths_by_tree_item = {}
        for db_id, tree_item in self._tree_items_by_db_id.items():
            if not self.GetChildrenCount(tree_item):
                self._leaf_elem_paths_by_tree_item[tree_item] = self.simhier.GetElemPath(db_id).replace('root.','')

        self._leaf_tree_items_by_elem_path = {v: k for k, v in self._leaf_elem_paths_by_tree_item.items()}
>>>>>>> Stashed changes
>>>>>>> Stashed changes

        self.Bind(wx.EVT_RIGHT_DOWN, self.__OnRightClick)
        self.Bind(wx.EVT_TREE_ITEM_EXPANDED, self.__OnItemExpanded)

<<<<<<< Updated upstream
        self._utiliz_image_list = frame.widget_renderer.utiliz_handler.CreateUtilizImageList()
        self.SetImageList(self._utiliz_image_list)

        self._tooltips_by_item = {}
        self.Bind(wx.EVT_TREE_ITEM_GETTOOLTIP, self.__ProcessTooltip)

        # Sanity checks to ensure that no element path contains 'root.'
        for _,elem_path in self._leaf_element_paths_by_tree_item.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._tree_items_by_elem_path.items():
            assert elem_path.find('root.') == -1

    def UpdateUtilizBitmaps(self):
=======
<<<<<<< Updated upstream
        self.__utiliz_image_list = frame.widget_renderer.utiliz_handler.CreateUtilizImageList()
        self.SetImageList(self.__utiliz_image_list)

    def UpdateUtilizBitmaps(self):
        for sim_path in self._container_sim_paths:
            utiliz_pct = self.frame.widget_renderer.utiliz_handler.GetUtilizPct(sim_path)
            image_idx = int(utiliz_pct * 100)
            item = self._tree_items_by_sim_path[sim_path]
            self.SetItemImage(item, image_idx)
=======
        self._utiliz_image_list = frame.widget_renderer.utiliz_handler.CreateUtilizImageList()
        self.SetImageList(self._utiliz_image_list)

        self._tooltips_by_item = {}
        self.Bind(wx.EVT_TREE_ITEM_GETTOOLTIP, self.__ProcessTooltip)

        # Sanity checks to ensure that no element path contains 'root.'
        for _,elem_path in self._leaf_elem_paths_by_tree_item.items():
            assert elem_path.find('root.') == -1

        for elem_path,_ in self._leaf_tree_items_by_elem_path.items():
            assert elem_path.find('root.') == -1

    def UpdateUtilizBitmaps(self):
>>>>>>> Stashed changes
        for elem_path in self.simhier.GetElemPaths():
            elem_id = self.simhier.GetElemID(elem_path)
            if self.simhier.GetWidgetType(elem_id) == 'QueueTable':
                utiliz_pct = self.frame.widget_renderer.utiliz_handler.GetUtilizPct(elem_path)
                image_idx = int(utiliz_pct * 100)
<<<<<<< Updated upstream
                item = self._tree_items_by_elem_path[elem_path]
=======
                item = self._leaf_tree_items_by_elem_path[elem_path]
>>>>>>> Stashed changes
                self.SetItemImage(item, image_idx)

                capacity = self.simhier.GetCapacityByElemPath(elem_path)
                size = int(capacity * utiliz_pct)
                tooltip = '{}\nUtilization: {}% ({}/{} bins filled)'.format(elem_path, round(utiliz_pct*100), size, capacity)
            elif self.simhier.GetWidgetType(elem_id) == 'Timeseries':
                image_idx = self._utiliz_image_list.GetImageCount() - 1
<<<<<<< Updated upstream
                item = self._tree_items_by_elem_path[elem_path]
=======
                item = self._leaf_tree_items_by_elem_path[elem_path]
>>>>>>> Stashed changes
                self.SetItemImage(item, image_idx)
                tooltip = '{}\nNo utilization data available for timeseries stats'.format(elem_path)
            else:
                item = None 
                tooltip = None

            if item and tooltip:
                self._tooltips_by_item[item] = tooltip
<<<<<<< Updated upstream
=======
>>>>>>> Stashed changes
>>>>>>> Stashed changes

    def ExpandAll(self):
        self.Unbind(wx.EVT_TREE_ITEM_EXPANDED)
        super(NavTree, self).ExpandAll()
        self.UpdateUtilizBitmaps()
        self.Bind(wx.EVT_TREE_ITEM_EXPANDED, self.__OnItemExpanded)

<<<<<<< Updated upstream
    def GetItemElemPath(self, item):
=======
<<<<<<< Updated upstream
    def GetItemSimPath(self, item):
>>>>>>> Stashed changes
        return self._leaf_element_paths_by_tree_item.get(item, None)
=======
    def GetItemElemPath(self, item):
        if not item or not item.IsOk():
            return None

        if item in self._leaf_elem_paths_by_tree_item:
            return self._leaf_elem_paths_by_tree_item[item]

        node_names = []
        while item and item != self.GetRootItem():
            node_name = self.GetItemText(item)
            node_names.append(node_name)
            item = self.GetItemParent(item)

        node_names.reverse()
        return '.'.join(node_names)
    
    def GetCurrentViewSettings(self):
        settings = {}
        settings['expanded_elem_paths'] = self.__GetExpandedElemPaths(self.GetRootItem())
        settings['selected_elem_path'] = self.GetItemElemPath(self.GetSelection())
        return settings

    def ApplyViewSettings(self, settings):
        expanded_elem_paths = settings['expanded_elem_paths']
        selected_elem_path = settings['selected_elem_path']

        self.CollapseAll()
        for elem_path in expanded_elem_paths:
            item = self._tree_items_by_elem_path[elem_path]
            self.Expand(item)

        if selected_elem_path:
            item = self._tree_items_by_elem_path[selected_elem_path]
            self.SelectItem(item)
            self.EnsureVisible(item)

    def __GetExpandedElemPaths(self, start_item):
        expanded_items = []
        if start_item.IsOk() and self.IsExpanded(start_item):
            if start_item != self.GetRootItem():
                elem_path = self.GetItemElemPath(start_item)
                expanded_items.append(elem_path)

            child = self.GetFirstChild(start_item)[0]
            while child.IsOk():
                expanded_items.extend(self.__GetExpandedElemPaths(child))
                child = self.GetNextSibling(child)

        return expanded_items
>>>>>>> Stashed changes

    def __RecurseBuildTree(self, parent_id):
        for child_id in self.simhier.GetChildIDs(parent_id):
            child_name = self.simhier.GetName(child_id)
            child = self.AppendItem(self._tree_items_by_db_id[parent_id], child_name)
            self._tree_items_by_db_id[child_id] = child
            self._tree_items_by_elem_path[self.simhier.GetElemPath(child_id)] = child
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

        if item in self._leaf_elem_paths_by_tree_item:
            elem_path = self._leaf_elem_paths_by_tree_item[item]
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

    def __ProcessTooltip(self, event):
        item = event.GetItem()
        event.SetToolTip(self._tooltips_by_item.get(item, ""))

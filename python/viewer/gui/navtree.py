import wx
from functools import partial
from viewer.gui.widgets.toolbase import ToolBase
from viewer.model.simhier import SimHierarchy

class NavTree(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(NavTree, self).__init__(parent, style=wx.TR_DEFAULT_STYLE | wx.TR_HIDE_ROOT | wx.TR_LINES_AT_ROOT)
        self.frame = frame
        self.simhier = SimHierarchy(frame.db)
        cursor = frame.db.cursor()

        self._root = self.AddRoot("root")
        self._sim_hier_root = self.AppendItem(self._root, "Sim Hierarchy")
        self._tree_items_by_db_id = {self.simhier.GetRootID(): self._sim_hier_root }
        self.__RecurseBuildTree(self.simhier.GetRootID())

        self._container_sim_paths = self.simhier.GetContainerSimPaths()
        self._leaf_element_paths_by_tree_item = {}
        for db_id, tree_item in self._tree_items_by_db_id.items():
            if not self.GetChildrenCount(tree_item):
                self._leaf_element_paths_by_tree_item[tree_item] = self.simhier.GetSimPath(db_id).replace('root.','')

        self._tree_items_by_sim_path = {v: k for k, v in self._leaf_element_paths_by_tree_item.items()}

        self.Bind(wx.EVT_RIGHT_DOWN, self.__OnRightClick)
        self.Bind(wx.EVT_TREE_BEGIN_DRAG, self.__OnBeginDrag)
        self.Bind(wx.EVT_TREE_ITEM_EXPANDED, self.__OnItemExpanded)

        self._tools_by_name = {}
        self._tools_by_item = {}
        self._widget_names_by_item = {}
        self._widget_factories_by_widget_name = {}

        self.__utiliz_image_list = frame.widget_renderer.utiliz_handler.CreateUtilizImageList()
        self.SetImageList(self.__utiliz_image_list)

    def PostLoad(self):
        for sim_path in self.simhier.GetScalarStatsSimPaths():
            item = self._tree_items_by_sim_path[sim_path]
            self.SetTreeNodeWidgetName(item, 'ScalarStatistic')

        for sim_path in self.simhier.GetScalarStructsSimPaths():
            item = self._tree_items_by_sim_path[sim_path]
            self.SetTreeNodeWidgetName(item, 'ScalarStruct')

        for sim_path in self.simhier.GetContainerSimPaths():
            item = self._tree_items_by_sim_path[sim_path]
            self.SetTreeNodeWidgetName(item, 'IterableStruct')

    def AddSystemWideTool(self, tool: ToolBase):
        # Find the 'Tools' node under the root
        tools_node = None
        item, cookie = self.GetFirstChild(self.GetRootItem())
        while item.IsOk():
            if self.GetItemText(item) == "Tools":
                tools_node = item
                break

            item, cookie = self.GetNextChild(self.GetRootItem(), cookie)

        if not tools_node:
            tools_node = self.AppendItem(self.GetRootItem(), "Tools")

        # Now look for the tool with the given name under the 'Tools' node
        name = tool.GetToolName()
        tool_node = None
        item, cookie = self.GetFirstChild(tools_node)
        while item.IsOk():
            if self.GetItemText(item) == name:
                tool_node = item
                break

            item, cookie = self.GetNextChild(tools_node, cookie)

        if not tool_node:
            self.AppendItem(tools_node, name)
            self._tools_by_name[name] = tool
        elif name in self._tools_by_name:
            if type(self._tools_by_name[name]) != type(tool):
                raise ValueError("Tool with name '%s' already exists and is of a different type" % name)
        else:
            self._tools_by_name[name] = tool

        self._tools_by_item = {}
        item, cookie = self.GetFirstChild(tools_node)
        while item.IsOk():
            self._tools_by_item[item] = self._tools_by_name[self.GetItemText(item)]
            item, cookie = self.GetNextChild(tools_node, cookie)

    def SetTreeNodeWidgetName(self, item, widget_name):
        self._widget_names_by_item[item] = widget_name
        
    def GetTool(self, tool_name):
        return self._tools_by_name.get(tool_name, None)
    
    def UpdateUtilizBitmaps(self):
        for sim_path in self._container_sim_paths:
            utiliz_pct = self.frame.widget_renderer.utiliz_handler.GetUtilizPct(sim_path)
            image_idx = int(utiliz_pct * 100)
            item = self._tree_items_by_sim_path[sim_path]
            self.SetItemImage(item, image_idx)

    def ExpandAll(self):
        self.Unbind(wx.EVT_TREE_ITEM_EXPANDED)
        super(NavTree, self).ExpandAll()
        self.UpdateUtilizBitmaps()
        self.Bind(wx.EVT_TREE_ITEM_EXPANDED, self.__OnItemExpanded)

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

        item_name = self.GetItemText(item)
        if item_name in self._tools_by_name:
            # Ensure that the item parent is the root.Tools node
            parent = self.GetItemParent(item)
            if self.GetItemText(parent) == 'Tools':
                parent = self.GetItemParent(parent)
                if parent == self.GetRootItem():
                    tool = self._tools_by_name[item_name]
                    help_text = tool.GetToolHelpText()
                    if help_text:
                        def OnHelp(event, **kwargs):
                            kwargs['tool'].ShowHelpDialog(kwargs['help_text'])

                        help_item = menu.Append(-1, 'See doc for {} tool'.format(item_name))
                        self.Bind(wx.EVT_MENU, partial(OnHelp, tool=tool, help_text=help_text), help_item)
                        self.PopupMenu(menu)
                        menu.Destroy()
                        return

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

    def __OnBeginDrag(self, event):
        item = event.GetItem()
        if item in self._tools_by_item:
            tool_name = self.GetItemText(item)
            data = wx.TextDataObject(tool_name)
            drop_source = wx.DropSource(self)
            drop_source.SetData(data)
            drop_source.DoDragDrop()
        elif item in self._widget_names_by_item:
            widget_name = self._widget_names_by_item[item]
            elem_path = self._leaf_element_paths_by_tree_item[item]
            data = wx.TextDataObject('{}${}'.format(widget_name, elem_path))
            drop_source = wx.DropSource(self)
            drop_source.SetData(data)
            drop_source.DoDragDrop()

    def __OnItemExpanded(self, event):
        self.UpdateUtilizBitmaps()

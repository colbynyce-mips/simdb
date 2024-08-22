import wx
from functools import partial
from viewer.gui.widgets.toolbase import ToolBase

class NavTree(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(NavTree, self).__init__(parent, style=wx.TR_DEFAULT_STYLE | wx.TR_HIDE_ROOT | wx.TR_LINES_AT_ROOT)
        self.frame = frame
        cursor = frame.db.cursor

        self._root = self.AddRoot("root")
        self._sim_hier_root = self.AppendItem(self._root, "Sim Hierarchy")
        self._tree_ids_by_id = {1: self._sim_hier_root }
        self.__RecurseBuildTree(1, cursor)

        # Find all the wx.TreeCtrl leaves
        leaves = []
        for node_id, tree_id in self._tree_ids_by_id.items():
            if not self.GetChildrenCount(tree_id):
                leaves.append(tree_id)

        # Get a mapping from the leaves to their full dot-delimited paths
        self._leaf_element_paths_by_tree_id = {}
        for leaf in leaves:
            path = []
            node = leaf
            while node != self._root:
                path.insert(0, self.GetItemText(node))
                node = self.GetItemParent(node)
            self._leaf_element_paths_by_tree_id[leaf] = '.'.join(path)

        # Get a mapping from the SimPath to the CollectionID from the CollectionElems table
        self._collection_id_by_sim_path = {}
        cursor.execute("SELECT CollectionID,SimPath FROM CollectionElems")
        rows = cursor.fetchall()
        for row in rows:
            self._collection_id_by_sim_path[row[1]] = row[0]

        # Iterate over the Collections table and find the DataType and IsContainer for each CollectionID
        self._data_type_by_collection_id = {}
        self._is_container_by_collection_id = {}
        cursor.execute("SELECT Id,DataType,IsContainer FROM Collections")
        rows = cursor.fetchall()
        for row in rows:
            self._data_type_by_collection_id[row[0]] = row[1]
            self._is_container_by_collection_id[row[0]] = row[2]

        self.Bind(wx.EVT_RIGHT_DOWN, self.__OnRightClick)
        self.Bind(wx.EVT_TREE_BEGIN_DRAG, self.__OnBeginDrag)
        self._tools_by_name = {}
        self._tools_by_item = {}

    def GetSelectionWidgetInfo(self):
        item = self.GetSelection()
        if item in self._leaf_element_paths_by_tree_id:
            elem_path = self._leaf_element_paths_by_tree_id[item]
            collection_id = self._collection_id_by_sim_path[elem_path]
            data_type = self._data_type_by_collection_id[collection_id]
            is_container = self._is_container_by_collection_id[collection_id]
            return (elem_path, collection_id, data_type, is_container)
        else:
            return None
        
    def GetLeafElementPaths(self):
        leaves = []
        for node_id, tree_id in self._tree_ids_by_id.items():
            if not self.GetChildrenCount(tree_id):
                leaves.append(tree_id)

        return [self._leaf_element_paths_by_tree_id[leaf] for leaf in leaves]
    
    def AddTool(self, tool: ToolBase):
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

    def __RecurseBuildTree(self, parent_id, cursor):
        cursor.execute("SELECT Id,Name FROM ElementTreeNodes WHERE ParentID = ?", (parent_id,))
        rows = cursor.fetchall()

        child_tree_ids_by_name = {}
        for row in rows:
            node_id = row[0]
            node_name = row[1]

            node = child_tree_ids_by_name.get(node_name, None)
            if not node:
                node = self.AppendItem(self._tree_ids_by_id[parent_id], node_name)

            self._tree_ids_by_id[node_id] = node

            self.__RecurseBuildTree(node_id, cursor)

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

        if item in self._leaf_element_paths_by_tree_id:
            elem_path = self._leaf_element_paths_by_tree_id[item]
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

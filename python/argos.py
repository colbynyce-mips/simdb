import wx
import sys
import sqlite3, json, tempfile
from simdb_collections import Collections

class MyFrame(wx.Frame):
    def __init__(self, *args, **kw):
        super(MyFrame, self).__init__(*args, **kw)

        self.InitUI()
    
    def InitUI(self):
        # Create a panel for the bottom of the frame
        self._playback_bar = PlaybackBar(self, self)
        
        # Create the SplitterWindow
        splitter = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
        
        # Create the TreeCtrl and Panel for the SplitterWindow
        self._navtree = NavTree(splitter, self)
        self._widget_panel = WidgetPanel(splitter, self)

        # Splitter configuration
        splitter.SplitVertically(self._navtree, self._widget_panel)
        
        # Add the SplitterWindow to the frame's sizer
        main_sizer = wx.BoxSizer(wx.VERTICAL)
        main_sizer.Add(splitter, 1, wx.EXPAND)
        main_sizer.Add(self._playback_bar, 0, wx.EXPAND | wx.ALL, 5)
        
        self.SetSizer(main_sizer)
        
        self.SetTitle("Argos")
        self.Maximize()

        splitter.SetSashPosition(200)
        self._playback_bar.SetMinSize((self.GetSize()[0], 50))

    @property
    def navtree(self):
        return self._navtree
    
    @property
    def playback_bar(self):
        return self._playback_bar
    
    @property
    def widget_panel(self):
        return self._widget_panel

class NavTree(wx.TreeCtrl):
    def __init__(self, parent, frame):
        super(NavTree, self).__init__(parent)

        self._frame = frame
        db_path = sys.argv[1]
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        self._root = self.AddRoot("root")
        self._tree_ids_by_id = {1: self._root}
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

        # Bind a callback to the tree control
        self.Bind(wx.EVT_TREE_SEL_CHANGED, self.__OnSelChanged)

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

    def __OnSelChanged(self, event):
        self._frame.widget_panel.RenderWidget()
        
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

class PlaybackBar(wx.Panel):
    def __init__(self, parent, frame):
        super(PlaybackBar, self).__init__(parent)

        self._frame = frame
        self.SetBackgroundColour('light gray')

        db_path = sys.argv[1]
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT TimeVal FROM CollectionData WHERE CollectionID=1')
        rows = cursor.fetchall()
        self._time_vals = [row[0] for row in rows]
        self._current_time = min(self._time_vals)

        self._goto_start = wx.Button(self, label="<<")
        self._step_backward = wx.Button(self, label="-1")
        self._playback_bar_label = wx.StaticText(self, label="Time: {}".format(self._current_time))
        self._step_forward = wx.Button(self, label="+1")
        self._goto_end = wx.Button(self, label=">>")

        # Bind the buttons to their respective callbacks
        self._goto_start.Bind(wx.EVT_BUTTON, self.__GotoStart)
        self._step_backward.Bind(wx.EVT_BUTTON, self.__StepBackward)
        self._step_forward.Bind(wx.EVT_BUTTON, self.__StepForward)
        self._goto_end.Bind(wx.EVT_BUTTON, self.__GotoEnd)

        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.AddStretchSpacer(1)
        sizer.Add(self._goto_start, 0, wx.ALIGN_CENTER | wx.ALL, 5)
        sizer.Add(self._step_backward, 0, wx.ALIGN_CENTER | wx.ALL, 5)
        sizer.Add(self._playback_bar_label, 0, wx.ALIGN_CENTER | wx.ALL, 5)
        sizer.Add(self._step_forward, 0, wx.ALIGN_CENTER | wx.ALL, 5)
        sizer.Add(self._goto_end, 0, wx.ALIGN_CENTER | wx.ALL, 5)
        sizer.AddStretchSpacer(1)

        self.SetSizer(sizer)

    @property
    def current_time(self):
        return self._current_time

    def __GotoStart(self, event):
        if self._current_time != self._time_vals[0]:
            self._current_time = self._time_vals[0]
            self._playback_bar_label.SetLabel("Time: {}".format(self._current_time))
            self._frame.widget_panel.RenderWidget()

    def __StepBackward(self, event):
        if self._current_time != self._time_vals[0]:
            self._current_time = self._time_vals[self._time_vals.index(self._current_time) - 1]
            self._playback_bar_label.SetLabel("Time: {}".format(self._current_time))
            self._frame.widget_panel.RenderWidget()

    def __StepForward(self, event):
        if self._current_time != self._time_vals[-1]:
            self._current_time = self._time_vals[self._time_vals.index(self._current_time) + 1]
            self._playback_bar_label.SetLabel("Time: {}".format(self._current_time))
            self._frame.widget_panel.RenderWidget()

    def __GotoEnd(self, event):
        if self._current_time != self._time_vals[-1]:
            self._current_time = self._time_vals[-1]
            self._playback_bar_label.SetLabel("Time: {}".format(self._current_time))
            self._frame.widget_panel.RenderWidget()

class WidgetPanel(wx.Panel):
    def __init__(self, parent, frame):
        super(WidgetPanel, self).__init__(parent)

        self._frame = frame
        self._collection_db = Collections(sys.argv[1])
        self._widget_text = wx.StaticText(self, label="No widget for this NavTree selection")
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self._widget_text, 0, wx.EXPAND | wx.ALL, 5)

        self.SetSizer(sizer)

    def RenderWidget(self):
        widget_info = self._frame.navtree.GetSelectionWidgetInfo()
        if widget_info:
            elem_path = widget_info[0]
            current_time = self._frame.playback_bar.current_time
            data_json = self._collection_db.Unpack(elem_path, current_time)
            del data_json['TimeVals']
            data_json = json.dumps(data_json, indent=4)
            self._widget_text.SetLabel(data_json)
        else:
            self._widget_text.SetLabel("No widget for this NavTree selection")

def main():
    app = wx.App(False)
    frame = MyFrame(None)
    frame.Show()
    app.MainLoop()

if __name__ == '__main__':
    main()

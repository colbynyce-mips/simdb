import wx
from viewer.gui.widgets.playback_bar import PlaybackBar
from viewer.gui.explorer import DataExplorer
from viewer.gui.inspector import DataInspector
from viewer.gui.widgets.queue_utiliz import QueueUtilizTool
from viewer.gui.widgets.packet_tracker import PacketTrackerTool
from viewer.gui.widgets.live_editor import LiveEditorTool
from viewer.gui.widgets.scalar_statistic import ScalarStatistic
from viewer.gui.widgets.scalar_struct import ScalarStruct
from viewer.gui.widgets.iterable_struct import IterableStruct

class ArgosFrame(wx.Frame):
    def __init__(self, view_settings, db):
        super().__init__(None, title=db.path)
        
        self.view_settings = view_settings
        self.db = db

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

    def PostLoad(self):
        self.explorer.navtree.AddSystemWideTool(QueueUtilizTool())
        self.explorer.navtree.AddSystemWideTool(PacketTrackerTool())
        self.explorer.navtree.AddSystemWideTool(LiveEditorTool())

        self.explorer.navtree.SetWidgetFactory('ScalarStatistic', ScalarStatistic.CreateWidget)
        self.explorer.navtree.SetWidgetFactory('ScalarStruct', ScalarStruct.CreateWidget)
        self.explorer.navtree.SetWidgetFactory('IterableStruct', IterableStruct.CreateWidget)

        leaves = []
        for node_id, tree_id in self.explorer.navtree._tree_ids_by_id.items():
            if not self.explorer.navtree.GetChildrenCount(tree_id):
                leaves.append(tree_id)

        for leaf in leaves:
            _, _, data_type, is_container = self.explorer.navtree.GetSelectionWidgetInfo(leaf)
            if data_type in ('int8_t', 'int16_t', 'int32_t', 'int64_t', 'uint8_t', 'uint16_t', 'uint32_t', 'uint64_t', 'float', 'double'):
                self.explorer.navtree.SetTreeNodeWidgetName(leaf, 'ScalarStatistic')
            elif not is_container:
                self.explorer.navtree.SetTreeNodeWidgetName(leaf, 'ScalarStruct')
            else:
                self.explorer.navtree.SetTreeNodeWidgetName(leaf, 'IterableStruct')

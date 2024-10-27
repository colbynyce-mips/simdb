import wx, copy, os
from viewer.gui.view_settings import DirtyReasons
from viewer.gui.widgets.grid import Grid

class SchedulingLinesWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame
        self.element_paths = []
        self.num_ticks_before = 5
        self.num_ticks_after = 50
        self.show_detailed_queue_packets = True
        self.grid = None
        self.info = None
        self.gear_btn = None

        cursor = frame.db.cursor()
        cmd = 'SELECT CollectionID,MaxSize FROM QueueMaxSizes'

        cursor.execute(cmd)
        self.queue_max_sizes_by_collection_id = {}
        for collection_id,max_size in cursor.fetchall():
            self.queue_max_sizes_by_collection_id[collection_id] = max_size

    def GetWidgetCreationString(self):
        return 'Scheduling Lines'

    def ErrorIfDroppedNodeIncompatible(self, elem_path):
        simhier = self.frame.simhier
        is_timeseries = elem_path in simhier.GetScalarStatsElemPaths()
        is_container = elem_path in simhier.GetContainerElemPaths()

        if not is_container:
            msg = 'Only leaf nodes that are containers (queues) can be dropped here.\n'
            msg += 'This node represents a scalar stat (timeseries).' if is_timeseries else 'This node represents a struct.'
            wx.MessageBox(msg, 'Incompatible Node', wx.OK | wx.ICON_ERROR)
            return True

        if elem_path in self.element_paths:
            wx.MessageBox('This queue is already being displayed.', 'Duplicate Queue', wx.OK | wx.ICON_ERROR)
            return True

        return False

    def AddElement(self, elem_path):
        self.element_paths.append(elem_path)
        self.__Refresh()

        if not self.gear_btn:
            self.gear_btn = wx.BitmapButton(self, bitmap=self.frame.CreateResourceBitmap('gear.png'), pos=(5,5))
            self.gear_btn.Bind(wx.EVT_BUTTON, self.__EditWidget)
            self.gear_btn.SetToolTip('Edit widget settings')

        self.frame.view_settings.SetDirty(reason=DirtyReasons.SchedulingLinesWidgetChanged)

    def UpdateWidgetData(self):
        if not self.grid and not self.info and not self.gear_btn:
            return

        self.__Refresh()

    def GetCurrentViewSettings(self):
        settings = {}
        settings['element_paths'] = copy.deepcopy(self.element_paths)
        settings['num_ticks_before'] = self.num_ticks_before
        settings['num_ticks_after'] = self.num_ticks_after
        settings['show_detailed_queue_packets'] = self.show_detailed_queue_packets
        return settings
    
    def GetCurrentUserSettings(self):
        return {}

    def ApplyViewSettings(self, settings):
        dirty = self.element_paths != settings['element_paths'] or \
                self.num_ticks_before != settings['num_ticks_before'] or \
                self.num_ticks_after != settings['num_ticks_after'] or \
                self.show_detailed_queue_packets != settings['show_detailed_queue_packets']

        if not dirty:
            return

        self.element_paths = copy.deepcopy(settings['element_paths'])
        self.num_ticks_before = settings['num_ticks_before']
        self.num_ticks_after = settings['num_ticks_after']
        self.show_detailed_queue_packets = settings['show_detailed_queue_packets']

        self.__Refresh()
        self.frame.view_settings.SetDirty(reason=DirtyReasons.SchedulingLinesWidgetChanged)

    def __Refresh(self):
        if len(self.element_paths):
            if self.info:
                self.info.Hide()

            self.SetBackgroundColour('white')
            self.__RegenerateSchedulingLinesGrid()

            if not self.gear_btn:
                self.gear_btn = wx.BitmapButton(self, bitmap=self.frame.CreateResourceBitmap('gear.png'), pos=(5,5))
                self.gear_btn.Bind(wx.EVT_BUTTON, self.__EditWidget)
                self.gear_btn.SetToolTip('Edit widget settings')
        else:
            self.__ShowUsageInfo()

    def __ShowUsageInfo(self):
        if self.gear_btn:
            self.gear_btn.Destroy()
            self.gear_btn = None

        if self.info:
            self.info.Destroy()
            self.info = None

        if self.grid:
            self.grid.Destroy()
            self.grid = None

        if self.GetSizer():
            self.GetSizer().Clear()

        self.SetBackgroundColour('light gray')

        self.info = wx.StaticText(self, label='Drag queues from the NavTree to create scheduling lines.', size=(600,18))
        self.info.SetFont(wx.Font(14, wx.FONTFAMILY_DEFAULT, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL))

        vsizer = wx.BoxSizer(wx.VERTICAL)
        vsizer.AddStretchSpacer()
        vsizer.Add(self.info, 1, wx.ALL | wx.CENTER | wx.EXPAND, 5)
        vsizer.AddStretchSpacer()

        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        hsizer.AddStretchSpacer()
        hsizer.Add(vsizer, 0, wx.CENTER | wx.EXPAND)
        hsizer.AddStretchSpacer()

        self.SetSizer(hsizer)
        self.Layout()

    def __RegenerateSchedulingLinesGrid(self):
        if self.grid:
            self.grid.Destroy()
            self.grid = None

        if self.info:
            self.info.Destroy()
            self.info = None

        if self.gear_btn:
            self.gear_btn.Destroy()
            self.gear_btn = None

        sizer = self.GetSizer()
        if sizer:
            sizer.Clear()

        sizer = wx.BoxSizer(wx.VERTICAL)

        # The number of rows can be calculated as:
        #  1. Go through each element path we are tracking (each is a queue)
        #     a. For each element path, get the number of bins in each queue (A)
        #     b. For each element path, note the maximum number of elements seen in the simulation (B)
        #     c. If A>B, then the number of rows is B+1. Otherwise, the number of rows is A. (C)
        #  >>> The required number of rows is the sum of all (C) values in the (1) loop.

        num_rows = 0
        for elem_path in self.element_paths:
            collection_id = self.frame.simhier.GetCollectionID(elem_path)
            num_bins = self.frame.simhier.GetCapacityByCollectionID(collection_id) # (A)
            max_size = self.queue_max_sizes_by_collection_id[collection_id]        # (B)
            elem_num_rows = max_size + 1 if max_size < num_bins else num_bins      # (C)
            num_rows += elem_num_rows

        # The number of columns can be calculated as:
        #  1. Start with the sum of self.num_ticks_before and self.num_ticks_after (A)
        #  2. Add 1 to (A) to account for the element paths column (captions)
        #  3. If self.show_detailed_queue_packets is True, add 3 to (A) to account for:
        #     a. A column to add a separator between the summary and detailed sections
        #     b. A column to duplicate the element paths column (captions)
        #     c. A column to show the stringified packet data e.g. "IntVal(4) DoubleVal(3.14)"

        num_cols = self.num_ticks_before + self.num_ticks_after + 1
        if self.show_detailed_queue_packets:
            num_cols += 3

        # Create 10-point monospace font for the grid labels and cells
        font = wx.Font(10, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)

        self.grid = Grid(self, self.frame, num_rows, num_cols, cell_font=font, label_font=font, cell_selection_allowed=False)
        self.gear_btn = wx.BitmapButton(self, bitmap=self.frame.CreateResourceBitmap('gear.png'))
        self.gear_btn.Bind(wx.EVT_BUTTON, self.__EditWidget)

        current_tick = self.frame.widget_renderer.tick
        col_labels = []
        for col in range(1, self.num_ticks_before + self.num_ticks_after + 1):
            tick = current_tick - self.num_ticks_before + col - 1
            self.grid.SetColLabelValue(col, str(tick))
            col_labels.append(str(tick))

        # Use a DC to get the length of the longest col label
        dc = wx.ScreenDC()
        dc.SetFont(self.grid.GetLabelFont())
        max_col_label_len = max([dc.GetTextExtent(col_label)[0] for col_label in col_labels])
        self.grid.SetColLabelSize(max_col_label_len + 4)

        self.grid.SetColLabelValue(0, '')
        self.grid.SetColLabelTextOrientation(wx.VERTICAL)
        self.grid.HideRowLabels()

        sizer.Add(self.gear_btn, 0, wx.ALL, 5)
        sizer.Add(self.grid, 0, wx.TOP | wx.EXPAND, 5)
        self.SetSizer(sizer)

        self.grid.ClearGrid()

        self.__SetElementCaptions(0)
        if self.show_detailed_queue_packets:
            self.__SetElementCaptions(self.num_ticks_before + self.num_ticks_after + 2)

            # Clear the column labels for the detailed queue packets section
            for i in range(self.num_ticks_before + self.num_ticks_after + 1, self.grid.GetNumberCols()):
                self.grid.SetColLabelValue(i, '')

        self.grid.AutoSize()
        self.Layout()

    def __SetElementCaptions(self, col):
        captions = []
        for elem_path in self.element_paths:
            captions.extend(self.__GetCaptionsForElement(elem_path))

        max_num_chars = max([len(caption) for caption in captions])
        row_offset = 0
        for elem_path in self.element_paths:
            row_offset += self.__SetCaptionsForElement(elem_path, row_offset, col, max_num_chars)

    def __SetCaptionsForElement(self, elem_path, row_offset, col, max_num_chars):
        collection_id = self.frame.simhier.GetCollectionID(elem_path)
        num_bins = self.frame.simhier.GetCapacityByCollectionID(collection_id)
        max_size = self.queue_max_sizes_by_collection_id[collection_id]

        if max_size < num_bins:
            caption = '{}[{}-{}]'.format(elem_path.split('.')[-1], max_size-1, num_bins-1)
            caption += ' '*(max_num_chars - len(caption))
            self.grid.SetCellValue(row_offset, col, caption)

            for i in range(1, max_size):
                bin_idx = max_size - i - 1
                caption = '{}[{}]'.format(elem_path.split('.')[-1], bin_idx)
                caption += ' '*(max_num_chars - len(caption))
                self.grid.SetCellValue(row_offset + i, col, caption)

            return max_size + 1
        else:
            for i in range(num_bins):
                bin_idx = num_bins - i - 1
                caption = '{}[{}]'.format(elem_path.split('.')[-1], bin_idx)
                caption += ' '*(max_num_chars - len(caption))
                self.grid.SetCellValue(row_offset + i, col, caption)

            return num_bins

    def __GetCaptionsForElement(self, elem_path):
        collection_id = self.frame.simhier.GetCollectionID(elem_path)
        num_bins = self.frame.simhier.GetCapacityByCollectionID(collection_id)
        max_size = self.queue_max_sizes_by_collection_id[collection_id]

        captions = []
        if max_size < num_bins:
            captions.append('{}[{}-{}]'.format(elem_path.split('.')[-1], max_size-1, num_bins-1))

            for i in range(1, max_size):
                bin_idx = max_size - i - 1
                captions.append('{}[{}]'.format(elem_path.split('.')[-1], bin_idx))
        else:
            for i in range(num_bins):
                bin_idx = num_bins - i - 1
                captions.append('{}[{}]'.format(elem_path.split('.')[-1], bin_idx))

        return captions

    def __EditWidget(self, evt):
        dlg = SchedulingLinesCustomizationDialog(self, self.element_paths, self.num_ticks_before, self.num_ticks_after, self.show_detailed_queue_packets)
        if dlg.ShowModal() == wx.ID_OK:
            self.ApplyViewSettings({'element_paths': dlg.GetElementPaths(),
                                    'num_ticks_before': dlg.GetNumTicksBefore(),
                                    'num_ticks_after': dlg.GetNumTicksAfter(),
                                    'show_detailed_queue_packets': dlg.ShowDetailedQueuePackets()})

        dlg.Destroy()

class SchedulingLinesCustomizationDialog(wx.Dialog):
    def __init__(self, parent, element_paths, num_ticks_before, num_ticks_after, show_detailed_queue_packets):
        super().__init__(parent, title="Customize Scheduling Lines")

        self.element_paths = copy.deepcopy(element_paths)
        self.num_ticks_before = num_ticks_before
        self.num_ticks_after = num_ticks_after
        self.show_detailed_queue_packets = show_detailed_queue_packets

        self.move_up_btn = wx.Button(self, label='Move Up')
        self.move_up_btn.Bind(wx.EVT_BUTTON, self.__MoveSelectedElemUp)

        self.move_down_btn = wx.Button(self, label='Move Down')
        self.move_down_btn.Bind(wx.EVT_BUTTON, self.__MoveSelectedElemDown)

        self.remove_btn = wx.Button(self, label='Remove')
        self.remove_btn.Bind(wx.EVT_BUTTON, self.__RemoveSelectedElems)

        edit_btns_sizer = wx.BoxSizer(wx.VERTICAL)
        edit_btns_sizer.Add(self.move_up_btn, 1, wx.ALL | wx.EXPAND, 5)
        edit_btns_sizer.Add(self.move_down_btn, 1, wx.BOTTOM | wx.EXPAND, 5)
        edit_btns_sizer.Add(self.remove_btn, 1, wx.BOTTOM | wx.EXPAND, 5)

        self.element_list = wx.ListBox(self, choices=self.element_paths, style=wx.LB_NEEDED_SB|wx.LB_MULTIPLE)
        self.element_list.Bind(wx.EVT_LISTBOX, self.__OnElementSelected)

        if len(self.element_paths) == 1:
            self.element_list.SetSelection(0)
            self.move_up_btn.Disable()
            self.move_down_btn.Disable()

        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        hsizer.Add(self.element_list, 1, wx.ALL | wx.EXPAND, 5)
        hsizer.Add(edit_btns_sizer, 0, wx.ALL | wx.EXPAND, 5)

        self.ok_btn = wx.Button(self, wx.ID_OK)
        self.cancel_btn = wx.Button(self, wx.ID_CANCEL)

        exit_btn_sizer = wx.BoxSizer(wx.HORIZONTAL)
        exit_btn_sizer.Add(self.ok_btn, 0, wx.ALL | wx.EXPAND, 5)
        exit_btn_sizer.Add(self.cancel_btn, 0, wx.ALL | wx.EXPAND, 5)

        num_ticks_before_text_label = wx.StaticText(self, label='Cycles to show before current tick:')
        num_ticks_after_text_label = wx.StaticText(self, label='Cycles to show after current tick:')

        num_ticks_before_text_ctrl = wx.TextCtrl(self, value=str(num_ticks_before))
        num_ticks_after_text_ctrl = wx.TextCtrl(self, value=str(num_ticks_after))
        num_ticks_before_text_ctrl.Bind(wx.EVT_TEXT, self.__OnNumTicksBeforeChanged)
        num_ticks_after_text_ctrl.Bind(wx.EVT_TEXT, self.__OnNumTicksAfterChanged)

        num_ticks_sizer = wx.FlexGridSizer(2, 2, 5, 5)
        num_ticks_sizer.Add(num_ticks_before_text_label, 0, wx.ALL | wx.EXPAND, 5)
        num_ticks_sizer.Add(num_ticks_before_text_ctrl, 0, wx.ALL | wx.EXPAND, 5)
        num_ticks_sizer.Add(num_ticks_after_text_label, 0, wx.ALL | wx.EXPAND, 5)
        num_ticks_sizer.Add(num_ticks_after_text_ctrl, 0, wx.ALL | wx.EXPAND, 5)

        show_detailed_queue_packets_checkbox = wx.CheckBox(self, label='Show detailed queue packets')
        show_detailed_queue_packets_checkbox.SetValue(show_detailed_queue_packets)
        show_detailed_queue_packets_checkbox.Bind(wx.EVT_CHECKBOX, self.__OnShowDetailedQueuePacketsChanged)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(hsizer, 1, wx.ALL | wx.EXPAND, 5)
        sizer.Add(num_ticks_sizer, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(show_detailed_queue_packets_checkbox, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(exit_btn_sizer, 0, wx.ALL | wx.EXPAND, 5)

        self.SetSizer(sizer)
        self.Layout()

        dc = wx.ScreenDC()
        dc.SetFont(self.element_list.GetFont())
        max_elem_path_len = max([dc.GetTextExtent(elem_path)[0] for elem_path in self.element_paths])
        max_elem_path_len = max(max_elem_path_len, dc.GetTextExtent(num_ticks_before_text_label.GetLabel())[0])
        max_elem_path_len = max(max_elem_path_len, dc.GetTextExtent(num_ticks_after_text_label.GetLabel())[0])

        dlg_min_width = max_elem_path_len + hsizer.CalcMin()[0] + 50
        dlg_min_height = sizer.CalcMin()[1] + 150
        self.SetSize((dlg_min_width, dlg_min_height))
        self.Layout()

    def GetElementPaths(self):
        return copy.deepcopy(self.element_paths)
    
    def GetNumTicksBefore(self):
        return self.num_ticks_before
    
    def GetNumTicksAfter(self):
        return self.num_ticks_after
    
    def ShowDetailedQueuePackets(self):
        return self.show_detailed_queue_packets

    def __OnElementSelected(self, evt):
        selected_elem_idxs = self.element_list.GetSelections()
        if len(selected_elem_idxs) != 1:
            self.move_up_btn.Disable()
            self.move_down_btn.Disable()

            if len(selected_elem_idxs) > 0:
                self.remove_btn.Enable()
            else:
                self.remove_btn.Disable()

            return

        selected_elem_idx = selected_elem_idxs[0]
        if selected_elem_idx == wx.NOT_FOUND:
            self.move_up_btn.Disable()
            self.move_down_btn.Disable()
            self.remove_btn.Disable()
            return
        else:
            self.remove_btn.Enable()

            if selected_elem_idx == 0:
                self.move_up_btn.Disable()
            else:
                self.move_up_btn.Enable()

            if selected_elem_idx == len(self.element_paths) - 1:
                self.move_down_btn.Disable()
            else:
                self.move_down_btn.Enable()

    def __MoveSelectedElemUp(self, evt):
        selected_elem_idxs = self.element_list.GetSelections()
        assert len(selected_elem_idxs) == 1
        selected_elem_idx = selected_elem_idxs[0]
        assert selected_elem_idx > 0

        orig_top_elem = self.element_paths[selected_elem_idx-1]
        orig_bottom_elem = self.element_paths[selected_elem_idx]
        self.element_paths[selected_elem_idx-1] = orig_bottom_elem
        self.element_paths[selected_elem_idx] = orig_top_elem

        self.element_list.SetString(selected_elem_idx-1, orig_bottom_elem)
        self.element_list.SetString(selected_elem_idx, orig_top_elem)
        self.element_list.SetSelection(selected_elem_idx-1)

    def __MoveSelectedElemDown(self, evt):
        selected_elem_idxs = self.element_list.GetSelections()
        assert len(selected_elem_idxs) == 1
        selected_elem_idx = selected_elem_idxs[0]
        assert selected_elem_idx > 0

        orig_top_elem = self.element_paths[selected_elem_idx]
        orig_bottom_elem = self.element_paths[selected_elem_idx+1]
        self.element_paths[selected_elem_idx] = orig_bottom_elem
        self.element_paths[selected_elem_idx+1] = orig_top_elem

        self.element_list.SetString(selected_elem_idx, orig_bottom_elem)
        self.element_list.SetString(selected_elem_idx+1, orig_top_elem)
        self.element_list.SetSelection(selected_elem_idx+1)

    def __RemoveSelectedElems(self, evt):
        selected_elem_idxs = self.element_list.GetSelections()
        element_paths = []

        for i,elem_path in enumerate(self.element_paths):
            if i not in selected_elem_idxs:
                element_paths.append(elem_path)

        self.element_paths = element_paths
        for i in range(len(self.element_paths)):
            self.element_list.SetString(i, self.element_paths[i])

        while self.element_list.GetCount() > len(self.element_paths):
            self.element_list.Delete(self.element_list.GetCount() - 1)

        for i in range(self.element_list.GetCount()):
            self.element_list.Deselect(i)

    def __OnNumTicksBeforeChanged(self, evt):
        if not self.__ValidateNumberOfTicks(evt.GetString(), (1,25), 'before'):
            return
        
        self.num_ticks_before = int(evt.GetString())

    def __OnNumTicksAfterChanged(self, evt):
        if not self.__ValidateNumberOfTicks(evt.GetString(), (5,100), 'after'):
            return
        
        self.num_ticks_after = int(evt.GetString())

    def __ValidateNumberOfTicks(self, num_ticks_str, allowed_range, before_or_after):
        try:
            num_ticks = int(num_ticks_str)
        except:
            self.ok_btn.Disable()
            self.ok_btn.SetToolTip("Invalid number of '{}' ticks entered. Must be an integer.".format(before_or_after))
            return False

        min_val, max_val = allowed_range

        if num_ticks < min_val:
            self.ok_btn.Disable()
            self.ok_btn.SetToolTip("Number of '{}' ticks cannot be less than 0.".format(before_or_after))
            return False
        
        if num_ticks > max_val:
            self.ok_btn.Disable()
            self.ok_btn.SetToolTip("Number of '{}' ticks cannot be greater than {}.".format(before_or_after, max_val))
            return False

        self.ok_btn.Enable()
        self.ok_btn.SetToolTip('')
        return True

    def __OnShowDetailedQueuePacketsChanged(self, evt):
        self.show_detailed_queue_packets = evt.IsChecked()

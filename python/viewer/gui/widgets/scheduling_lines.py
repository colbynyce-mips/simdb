import wx, copy, re
from collections import OrderedDict
from viewer.gui.view_settings import DirtyReasons
from viewer.gui.widgets.grid import Grid

class SchedulingLinesWidget(wx.Panel):
    def __init__(self, parent, frame):
        super().__init__(parent)
        self.frame = frame
        self.num_ticks_before = 5
        self.num_ticks_after = 50
        self.show_detailed_queue_packets = True
        self.caption_mgr = CaptionManager(frame.simhier)
        self.grid = None
        self.info = None
        self.gear_btn = None

        cursor = frame.db.cursor()
        cmd = 'SELECT CollectionID,MaxSize FROM QueueMaxSizes'

        cursor.execute(cmd)
        self.queue_max_sizes_by_collection_id = {}
        for collection_id,max_size in cursor.fetchall():
            self.queue_max_sizes_by_collection_id[collection_id] = max_size

        self.__ShowUsageInfo()

    def GetWidgetCreationString(self):
        return 'Scheduling Lines'

    def GetErrorIfDroppedNodeIncompatible(self, elem_path):
        simhier = self.frame.simhier
        is_timeseries = elem_path in simhier.GetScalarStatsElemPaths()
        is_container = elem_path in simhier.GetContainerElemPaths()

        if not is_container:
            msg = 'Only leaf nodes that are containers (queues) can be dropped here. '
            msg += 'This node represents a scalar stat (timeseries).' if is_timeseries else 'This node represents a struct.'
            return msg, 'Incompatible Node'

        if elem_path in self.caption_mgr.GetAllMatchingElemPaths():
            return 'This queue is already being displayed.', 'Duplicate Queue'
        
        if self.grid:
            existing_captions = set()
            for row in range(self.grid.GetNumberRows()):
                existing_captions.add(self.grid.GetCellValue(row, 0))

            todo_captions = self.__GetCaptionsForElement(elem_path)
            for caption in todo_captions:
                if caption in existing_captions:
                    msg = 'Adding this to the Scheduling Lines widget would result in a duplicate caption(s). '
                    msg += 'You need to open the widget settings dialog and adjust the regexes.'
                    return msg, 'Duplicate Caption'

        return None

    def AddElement(self, elem_path):
        self.__AddElement(elem_path)
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
        settings['regexes'] = self.caption_mgr.GetElemPathRegexReplacements(as_list=True)
        settings['num_ticks_before'] = self.num_ticks_before
        settings['num_ticks_after'] = self.num_ticks_after
        settings['show_detailed_queue_packets'] = self.show_detailed_queue_packets
        return settings
    
    def GetCurrentUserSettings(self):
        return {}

    def ApplyViewSettings(self, settings):
        dirty = self.caption_mgr.GetElemPathRegexReplacements(as_list=True) != settings['regexes'] or \
                self.num_ticks_before != settings['num_ticks_before'] or \
                self.num_ticks_after != settings['num_ticks_after'] or \
                self.show_detailed_queue_packets != settings['show_detailed_queue_packets']

        if not dirty:
            return

        self.caption_mgr.SetElemPathRegexReplacements(settings['regexes'])
        self.num_ticks_before = settings['num_ticks_before']
        self.num_ticks_after = settings['num_ticks_after']
        self.show_detailed_queue_packets = settings['show_detailed_queue_packets']

        self.__Refresh()
        self.frame.view_settings.SetDirty(reason=DirtyReasons.SchedulingLinesWidgetChanged)

    def __AddElement(self, elem_path):
        assert elem_path not in self.caption_mgr.GetAllMatchingElemPaths()
            
        # The default behavior is to take an element path like this:
        #   top.core0.rob.stats.num_insts_retired
        #
        # And use the caption replacement:
        #   NumInstsRetired
        #
        # Which results in captions like this (assume queue has capacity of 4):
        #
        #   NumInstsRetired[3]
        #   NumInstsRetired[2]
        #   NumInstsRetired[1]
        #   NumInstsRetired[0]
        #
        # A complete example might be to also use the core index in the caption,
        # which would change the regex to:
        #
        #   top.core([0-9]+).rob.stats.num_insts_retired
        #
        # And use the caption replacement:
        #
        #   NumInstsRetired\1
        #
        # Which results in captions like this (assume queue has capacity of 4):
        #
        #   NumInstsRetired0[3]
        #   NumInstsRetired0[2]
        #   NumInstsRetired0[1]
        #   NumInstsRetired0[0]
        #
        #   NumInstsRetired1[3]
        #   NumInstsRetired1[2]
        #   NumInstsRetired1[1]
        #   NumInstsRetired1[0]
        #
        # The user can adjust these settings in the widget settings dialog.
        regex_replacement = GetHeadsUpCamelCaseQueueName(elem_path)
        self.caption_mgr.AddElemPathRegexReplacement(elem_path, regex_replacement)

    def __Refresh(self):
        if len(self.caption_mgr.GetAllMatchingElemPaths()) > 0:
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
        for elem_path in self.caption_mgr.GetAllMatchingElemPaths():
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
        for elem_path in self.caption_mgr.GetAllMatchingElemPaths():
            captions.extend(self.__GetCaptionsForElement(elem_path))

        max_num_chars = max([len(caption) for caption in captions])
        row_offset = 0
        for elem_path in self.caption_mgr.GetAllMatchingElemPaths():
            row_offset += self.__SetCaptionsForElement(elem_path, row_offset, col, max_num_chars)

    def __SetCaptionsForElement(self, elem_path, row_offset, col, max_num_chars):
        collection_id = self.frame.simhier.GetCollectionID(elem_path)
        num_bins = self.frame.simhier.GetCapacityByCollectionID(collection_id)
        max_size = self.queue_max_sizes_by_collection_id[collection_id]

        if max_size < num_bins:
            caption_prefix = self.caption_mgr.GetCaptionPrefix(elem_path)
            caption = '{}[{}-{}]'.format(caption_prefix, max_size-1, num_bins-1)
            caption += ' '*(max_num_chars - len(caption))
            self.grid.SetCellValue(row_offset, col, caption)

            for i in range(1, max_size):
                bin_idx = max_size - i - 1
                #caption = '{}[{}]'.format(caption_prefix, bin_idx)
                #caption += ' '*(max_num_chars - len(caption))
                caption = self.caption_mgr.GetCaption(elem_path, bin_idx)
                caption += ' '*(max_num_chars - len(caption))
                self.grid.SetCellValue(row_offset + i, col, caption)

            return max_size + 1
        else:
            for i in range(num_bins):
                bin_idx = num_bins - i - 1
                #caption = '{}[{}]'.format(caption_prefix, bin_idx)
                #caption += ' '*(max_num_chars - len(caption))
                caption = self.caption_mgr.GetCaption(elem_path, bin_idx)
                caption += ' '*(max_num_chars - len(caption))
                self.grid.SetCellValue(row_offset + i, col, caption)

            return num_bins

    def __GetCaptionsForElement(self, elem_path):
        collection_id = self.frame.simhier.GetCollectionID(elem_path)
        num_bins = self.frame.simhier.GetCapacityByCollectionID(collection_id)
        max_size = self.queue_max_sizes_by_collection_id[collection_id]

        captions = []
        if max_size < num_bins:
            caption_prefix = self.caption_mgr.GetCaptionPrefix(elem_path)
            captions.append('{}[{}-{}]'.format(caption_prefix, max_size-1, num_bins-1))

            for i in range(1, max_size):
                bin_idx = max_size - i - 1
                #captions.append('{}[{}]'.format(caption_prefix, bin_idx))
                caption = self.caption_mgr.GetCaption(elem_path, bin_idx)
                captions.append(caption)
        else:
            for i in range(num_bins):
                bin_idx = num_bins - i - 1
                #captions.append('{}[{}]'.format(caption_prefix, bin_idx))
                caption = self.caption_mgr.GetCaption(elem_path, bin_idx)
                captions.append(caption)

        return captions

    def __EditWidget(self, evt):
        dlg = SchedulingLinesCustomizationDialog(self, self.caption_mgr, self.num_ticks_before, self.num_ticks_after, self.show_detailed_queue_packets)
        result = dlg.ShowModal()
        dlg.Destroy()

        if result == wx.ID_OK:
            self.ApplyViewSettings({'regexes': dlg.GetElementPathCaptionRegexes(as_list=True),
                                    'num_ticks_before': dlg.GetNumTicksBefore(),
                                    'num_ticks_after': dlg.GetNumTicksAfter(),
                                    'show_detailed_queue_packets': dlg.ShowDetailedQueuePackets()})

class SchedulingLinesCustomizationDialog(wx.Dialog):
    def __init__(self, parent, caption_mgr, num_ticks_before, num_ticks_after, show_detailed_queue_packets):
        super().__init__(parent, title="Customize Scheduling Lines")

        self.caption_mgr = copy.deepcopy(caption_mgr)
        self.show_detailed_queue_packets = show_detailed_queue_packets
        self.pending_list_ctrl_changes = []

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

        self.element_path_regexes_list_ctrl = wx.ListCtrl(self, style=wx.LC_REPORT)
        self.element_path_regexes_list_ctrl.InsertColumn(0, "Path Regex")
        self.element_path_regexes_list_ctrl.InsertColumn(1, "Caption")
        self.element_path_regexes_list_ctrl.Bind(wx.EVT_LIST_ITEM_SELECTED, self.__OnElementSelected)
        self.element_path_regexes_list_ctrl.Bind(wx.EVT_LEFT_DCLICK, self.__OnElementDoubleClicked)

        element_path_caption_regexes = self.caption_mgr.GetElemPathRegexReplacements()
        idx = 0
        for path_regex, caption_replacements in element_path_caption_regexes.items():
            self.element_path_regexes_list_ctrl.InsertItem(idx, path_regex)
            self.element_path_regexes_list_ctrl.SetItem(idx, 1, caption_replacements)

            if len(element_path_caption_regexes) == 1:
                self.move_up_btn.Disable()
                self.move_down_btn.Disable()
            else:
                idx += 1

        hsizer = wx.BoxSizer(wx.HORIZONTAL)
        hsizer.Add(self.element_path_regexes_list_ctrl, 1, wx.ALL | wx.EXPAND, 5)
        hsizer.Add(edit_btns_sizer, 0, wx.ALL | wx.EXPAND, 5)

        self.ok_btn = wx.Button(self, wx.ID_OK)
        self.cancel_btn = wx.Button(self, wx.ID_CANCEL)

        exit_btn_sizer = wx.BoxSizer(wx.HORIZONTAL)
        exit_btn_sizer.Add(self.ok_btn, 0, wx.ALL | wx.EXPAND, 5)
        exit_btn_sizer.Add(self.cancel_btn, 0, wx.ALL | wx.EXPAND, 5)

        num_ticks_before_min_val = 1
        num_ticks_before_max_val = 25
        num_ticks_after_min_val = 5
        num_ticks_after_max_val = 100

        num_ticks_before_info_label = wx.StaticText(self, label='Cycles to show before current tick:')
        num_ticks_after_info_label = wx.StaticText(self,  label='Cycles to show after current tick:')

        self.num_ticks_before_value_label = wx.StaticText(self, label=str(num_ticks_before))
        self.num_ticks_after_value_label = wx.StaticText(self, label=str(num_ticks_after))

        num_ticks_before_min_val_text_label = wx.StaticText(self, label=str(num_ticks_before_min_val))
        num_ticks_before_max_val_text_label = wx.StaticText(self, label=str(num_ticks_before_max_val))
        num_ticks_after_min_val_text_label = wx.StaticText(self, label=str(num_ticks_after_min_val))
        num_ticks_after_max_val_text_label = wx.StaticText(self, label=str(num_ticks_after_max_val))

        self.num_ticks_before_slider = wx.Slider(self, minValue=num_ticks_before_min_val, maxValue=num_ticks_before_max_val, value=num_ticks_before)
        self.num_ticks_after_slider = wx.Slider(self, minValue=num_ticks_after_min_val, maxValue=num_ticks_after_max_val, value=num_ticks_after)

        self.num_ticks_before_slider.Bind(wx.EVT_SCROLL, self.__OnNumTicksBeforeSliderChanged)
        self.num_ticks_after_slider.Bind(wx.EVT_SCROLL, self.__OnNumTicksAfterSliderChanged)

        w,h = self.num_ticks_before_slider.GetSize()
        self.num_ticks_before_slider.SetMinSize((300, h))

        w,h = self.num_ticks_after_slider.GetSize()
        self.num_ticks_after_slider.SetMinSize((300, h))

        num_ticks_sizer = wx.FlexGridSizer(rows=2, cols=6, hgap=0, vgap=0)
        num_ticks_sizer.Add(num_ticks_before_info_label)
        num_ticks_sizer.Add(self.num_ticks_before_value_label, 0, wx.LEFT, 20)
        num_ticks_sizer.Add(wx.StaticLine(self, style=wx.LI_VERTICAL), 0, wx.LEFT | wx.EXPAND, 20)
        num_ticks_sizer.Add(num_ticks_before_min_val_text_label, 0, wx.LEFT, 20)
        num_ticks_sizer.Add(self.num_ticks_before_slider, 0, wx.TOP | wx.EXPAND, -7)
        num_ticks_sizer.Add(num_ticks_before_max_val_text_label)

        num_ticks_sizer.Add(num_ticks_after_info_label)
        num_ticks_sizer.Add(self.num_ticks_after_value_label, 0, wx.LEFT, 20)
        num_ticks_sizer.Add(wx.StaticLine(self, style=wx.LI_VERTICAL), 0, wx.LEFT | wx.EXPAND, 20)
        num_ticks_sizer.Add(num_ticks_after_min_val_text_label, 0, wx.LEFT, 20)
        num_ticks_sizer.Add(self.num_ticks_after_slider, 0, wx.TOP | wx.EXPAND, -7)
        num_ticks_sizer.Add(num_ticks_after_max_val_text_label)

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
        dc.SetFont(self.element_path_regexes_list_ctrl.GetFont())
        col0_width = max([dc.GetTextExtent(s)[0] for s in element_path_caption_regexes.keys()]) + 50
        col1_width = max([dc.GetTextExtent(s)[0] for s in element_path_caption_regexes.values()]) + 50

        col0_width += dc.GetTextExtent('([0-9]+)')[0]

        self.element_path_regexes_list_ctrl.SetColumnWidth(0, col0_width)
        self.element_path_regexes_list_ctrl.SetColumnWidth(1, col1_width)

        dlg_width  = col0_width + col1_width
        dlg_min_width = dlg_width + hsizer.CalcMin()[0] + 50
        dlg_min_height = sizer.CalcMin()[1] + 150
        self.SetSize((dlg_min_width, dlg_min_height))
        self.Layout()

    @property
    def frame(self):
        return self.GetParent().frame

    def GetNumTicksBefore(self):
        return self.num_ticks_before_slider.GetValue()

    def GetNumTicksAfter(self):
        return self.num_ticks_after_slider.GetValue()

    def ShowDetailedQueuePackets(self):
        return self.show_detailed_queue_packets
    
    def GetElementPathCaptionRegexes(self, as_list=False):
        regexes = OrderedDict()
        for row in range(self.element_path_regexes_list_ctrl.GetItemCount()):
            path_regex = self.element_path_regexes_list_ctrl.GetItemText(row, 0)
            caption_replacements = self.element_path_regexes_list_ctrl.GetItemText(row, 1)
            regexes[path_regex] = caption_replacements

        if as_list:
            return list(regexes.items())

        return regexes

    def __OnElementSelected(self, evt):
        selected_elem_idxs = self.__GetListCtrlSelectedItemIdxs()
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

            if selected_elem_idx == self.element_path_regexes_list_ctrl.GetItemCount() - 1:
                self.move_down_btn.Disable()
            else:
                self.move_down_btn.Enable()

    def __MoveSelectedElemUp(self, evt):
        selected_elem_idxs = self.__GetListCtrlSelectedItemIdxs()
        assert len(selected_elem_idxs) == 1
        selected_elem_idx = selected_elem_idxs[0]
        assert selected_elem_idx > 0

        orig_top_idx = selected_elem_idx-1
        orig_bottom_idx = selected_elem_idx

        orig_top_item_col0_text = self.element_path_regexes_list_ctrl.GetItemText(orig_top_idx, 0)
        orig_top_item_col1_text = self.element_path_regexes_list_ctrl.GetItemText(orig_top_idx, 1)

        orig_bottom_item_col0_text = self.element_path_regexes_list_ctrl.GetItemText(orig_bottom_idx, 0)
        orig_bottom_item_col1_text = self.element_path_regexes_list_ctrl.GetItemText(orig_bottom_idx, 1)

        self.element_path_regexes_list_ctrl.SetItem(orig_top_idx, 0, orig_bottom_item_col0_text)
        self.element_path_regexes_list_ctrl.SetItem(orig_top_idx, 1, orig_bottom_item_col1_text)

        self.element_path_regexes_list_ctrl.SetItem(orig_bottom_idx, 0, orig_top_item_col0_text)
        self.element_path_regexes_list_ctrl.SetItem(orig_bottom_idx, 1, orig_top_item_col1_text)

        self.element_path_regexes_list_ctrl.Select(orig_bottom_idx, False)
        self.element_path_regexes_list_ctrl.Select(orig_top_idx, True)

        element_path_caption_regexes = self.GetElementPathCaptionRegexes(as_list=True)
        tmp = element_path_caption_regexes[orig_top_idx]
        element_path_caption_regexes[orig_top_idx] = element_path_caption_regexes[orig_bottom_idx]
        element_path_caption_regexes[orig_bottom_idx] = tmp
        self.caption_mgr.SetElemPathRegexReplacements(element_path_caption_regexes)

    def __MoveSelectedElemDown(self, evt):
        selected_elem_idxs = self.__GetListCtrlSelectedItemIdxs()
        assert len(selected_elem_idxs) == 1
        selected_elem_idx = selected_elem_idxs[0]
        assert selected_elem_idx < self.element_path_regexes_list_ctrl.GetItemCount() - 1

        orig_top_idx = selected_elem_idx
        orig_bottom_idx = selected_elem_idx+1

        orig_top_item_col0_text = self.element_path_regexes_list_ctrl.GetItemText(orig_top_idx, 0)
        orig_top_item_col1_text = self.element_path_regexes_list_ctrl.GetItemText(orig_top_idx, 1)

        orig_bottom_item_col0_text = self.element_path_regexes_list_ctrl.GetItemText(orig_bottom_idx, 0)
        orig_bottom_item_col1_text = self.element_path_regexes_list_ctrl.GetItemText(orig_bottom_idx, 1)

        self.element_path_regexes_list_ctrl.SetItem(orig_top_idx, 0, orig_bottom_item_col0_text)
        self.element_path_regexes_list_ctrl.SetItem(orig_top_idx, 1, orig_bottom_item_col1_text)

        self.element_path_regexes_list_ctrl.SetItem(orig_bottom_idx, 0, orig_top_item_col0_text)
        self.element_path_regexes_list_ctrl.SetItem(orig_bottom_idx, 1, orig_top_item_col1_text)

        self.element_path_regexes_list_ctrl.Select(orig_top_idx, False)
        self.element_path_regexes_list_ctrl.Select(orig_bottom_idx, True)

        element_path_caption_regexes = self.GetElementPathCaptionRegexes(as_list=True)
        tmp = element_path_caption_regexes[orig_top_idx]
        element_path_caption_regexes[orig_top_idx] = element_path_caption_regexes[orig_bottom_idx]
        element_path_caption_regexes[orig_bottom_idx] = tmp
        self.caption_mgr.SetElemPathRegexReplacements(element_path_caption_regexes)

    def __RemoveSelectedElems(self, evt):
        selected_elem_idxs = self.__GetListCtrlSelectedItemIdxs()
        selected_elem_idxs.reverse()

        for i in selected_elem_idxs:
            self.element_path_regexes_list_ctrl.DeleteItem(i)

        for i in range(self.element_path_regexes_list_ctrl.GetItemCount()):
            self.element_path_regexes_list_ctrl.Select(i, False)

        element_path_caption_regexes = self.GetElementPathCaptionRegexes()
        self.caption_mgr.SetElemPathRegexReplacements(element_path_caption_regexes)

    def __GetListCtrlSelectedItemIdxs(self):
        idxs = []
        item_count = self.element_path_regexes_list_ctrl.GetItemCount()

        for i in range(item_count):
            if self.element_path_regexes_list_ctrl.GetItemState(i, wx.LIST_STATE_SELECTED):
                idxs.append(i)

        return idxs

    def __OnShowDetailedQueuePacketsChanged(self, evt):
        self.show_detailed_queue_packets = evt.IsChecked()

    def __OnElementDoubleClicked(self, evt):
        # Get the mouse position in the list control
        mouse_x, mouse_y = evt.GetPosition()

        hit = self.element_path_regexes_list_ctrl.HitTest((mouse_x, mouse_y))
        if not hit or hit[0] == -1:
            return
        
        row = hit[0]
        if mouse_x < self.element_path_regexes_list_ctrl.GetColumnWidth(0):
            col = 0
        elif mouse_x > self.element_path_regexes_list_ctrl.GetColumnWidth(0):
            col = 1
        else:
            return

        row_rect = self.element_path_regexes_list_ctrl.GetItemRect(row)
        if col == 0:
            cell_rect = wx.Rect(row_rect.GetLeft(),
                                row_rect.GetTop(),
                                self.element_path_regexes_list_ctrl.GetColumnWidth(0),
                                row_rect.GetHeight())
        else:
            cell_rect = wx.Rect(row_rect.GetLeft() + self.element_path_regexes_list_ctrl.GetColumnWidth(0),
                                row_rect.GetTop(),
                                self.element_path_regexes_list_ctrl.GetColumnWidth(1),
                                row_rect.GetHeight())

        pos = cell_rect.GetTopLeft()
        size = cell_rect.GetSize()
        self.__EditListCtrlCell(row, col, pos, size)

    def __EditListCtrlCell(self, row, col, text_ctrl_pos, text_ctrl_size):
        # Get the current value of the cell
        current_text = self.element_path_regexes_list_ctrl.GetItemText(row, col)

        # Create a text control to edit the cell
        self.text_ctrl = wx.TextCtrl(self, value=current_text, style=wx.TE_PROCESS_ENTER, pos=text_ctrl_pos, size=text_ctrl_size)

        # Callbacks when finished editing the cell
        self.text_ctrl.Bind(wx.EVT_TEXT_ENTER, lambda evt: self.__OnListCtrlEditComplete(evt, row, col, current_text))
        self.text_ctrl.Bind(wx.EVT_KILL_FOCUS, lambda evt: self.__OnListCtrlEditComplete(evt, row, col, current_text))

        # Focus the text control right away
        self.text_ctrl.SetFocus()

    def __OnListCtrlEditComplete(self, evt, row, col, orig_text):
        if not self.text_ctrl:
            return
        else:
            self.text_ctrl.Unbind(wx.EVT_TEXT_ENTER)
            self.text_ctrl.Unbind(wx.EVT_KILL_FOCUS)
            evt.Skip()

        new_value = self.text_ctrl.GetValue()

        def DestroyTextCtrl(text_ctrl):
            text_ctrl.Destroy()
            text_ctrl = None

        wx.CallAfter(DestroyTextCtrl, self.text_ctrl)

        def ResetListCtrl(list_ctrl, row, col, text):
            list_ctrl.SetItem(row, col, text)

        if col == 0:
            try:
                re.compile(new_value)

                # Just because the regex compiles doesn't mean it's valid. We need to test it.
                caption_replacement = self.element_path_regexes_list_ctrl.GetItemText(row, 1)
                has_matches = False
                has_substitutions = False

                for elem_path in self.frame.simhier.GetContainerElemPaths():
                    if re.compile(new_value).match(elem_path):
                        has_matches = True
                        try:
                            re.sub(new_value, caption_replacement, elem_path)
                            has_substitutions = True
                            break
                        except:
                            pass

                if not has_matches:
                    tooltip = 'There are no element paths that match this regex:\n\n{}\n\n'.format(new_value)
                    tooltip += 'Please remove it in the settings dialog.'
                    self.ok_btn.Disable()
                    self.ok_btn.SetToolTip(tooltip)
                    wx.CallAfter(ResetListCtrl, self.element_path_regexes_list_ctrl, row, col, orig_text)
                elif not has_substitutions:
                    tooltip =  'The regex "{}" is valid syntax but there are no element paths that '
                    tooltip += 'can be substituted with the caption replacement "{}". Please fix this '
                    tooltip += 'in the settings dialog.'
                    tooltip = tooltip.format(new_value, caption_replacement)
                    self.ok_btn.Disable()
                    self.ok_btn.SetToolTip(tooltip)
                else:
                    self.ok_btn.Enable()
                    self.ok_btn.SetToolTip('')

                self.pending_list_ctrl_changes.append((row, col, new_value))
                wx.CallAfter(self.__ApplyPendingListCtrlChanges)
            except re.error:
                tooltip = 'Invalid regular expression entered:\n\n{}\n\nPlease fix it in the widget settings dialog.'.format(new_value)
                self.ok_btn.Disable()
                self.ok_btn.SetToolTip(tooltip)
                wx.CallAfter(ResetListCtrl, self.element_path_regexes_list_ctrl, row, col, orig_text)
        else:
            # Ensure the regex is valid for this caption substitution
            regex = self.element_path_regexes_list_ctrl.GetItemText(row, 0)
            has_matches = False
            has_substitutions = False

            for elem_path in self.frame.simhier.GetContainerElemPaths():
                try:
                    if re.compile(regex).match(elem_path):
                        has_matches = True
                        try:
                            re.sub(regex, new_value, elem_path)
                            has_substitutions = True
                            break
                        except:
                            pass
                except re.error:
                    pass

            if not has_matches:
                tooltip =  'There are no element paths that match this regex:\n\n{}\n\n'
                tooltip += 'Please remove it in the settings dialog.'
                tooltip =  tooltip.format(regex)
                self.ok_btn.Disable()
                self.ok_btn.SetToolTip(tooltip)
            elif not has_substitutions:
                tooltip =  'The regex "{}" is valid syntax but there are no element paths that\n'
                tooltip += 'can be substituted with the caption replacement "{}".'
                tooltip =  tooltip.format(regex, new_value)
                self.ok_btn.Disable()
                self.ok_btn.SetToolTip(tooltip)
                wx.CallAfter(ResetListCtrl, self.element_path_regexes_list_ctrl, row, col, orig_text)
            else:
                self.ok_btn.Enable()
                self.ok_btn.SetToolTip('')

            self.pending_list_ctrl_changes.append((row, col, new_value))
            wx.CallAfter(self.__ApplyPendingListCtrlChanges)

    def __ApplyPendingListCtrlChanges(self):
        for row, col, new_value in self.pending_list_ctrl_changes:
            self.element_path_regexes_list_ctrl.SetItem(row, col, new_value)

        self.pending_list_ctrl_changes = []

    def __OnNumTicksBeforeSliderChanged(self, evt):
        self.num_ticks_before_value_label.SetLabel(str(self.num_ticks_before_slider.GetValue()))

    def __OnNumTicksAfterSliderChanged(self, evt):
        self.num_ticks_after_value_label.SetLabel(str(self.num_ticks_after_slider.GetValue()))

class CaptionManager:
    def __init__(self, simhier):
        self.simhier = simhier
        self.regex_replacements_by_elem_path_regex = OrderedDict()

    def AddElemPathRegexReplacement(self, elem_path_regex, regex_replacement):
        self.regex_replacements_by_elem_path_regex[elem_path_regex] = regex_replacement

    def SetElemPathRegexReplacements(self, regex_replacements_by_elem_path_regex):
        if isinstance(regex_replacements_by_elem_path_regex, list):
            regex_replacements_by_elem_path_regex = OrderedDict(regex_replacements_by_elem_path_regex)
        elif isinstance(regex_replacements_by_elem_path_regex, dict):
            raise TypeError('Must be a list or an OrderedDict, not a regular unordered python dict.')

        assert isinstance(regex_replacements_by_elem_path_regex, OrderedDict)
        self.regex_replacements_by_elem_path_regex = copy.deepcopy(regex_replacements_by_elem_path_regex)

    def GetElemPathRegexReplacements(self, as_list=False):
        d = copy.deepcopy(self.regex_replacements_by_elem_path_regex)
        if as_list:
            return list(d.items())

        return d

    def GetCaption(self, elem_path, bin_idx):
        for regex, replacements in self.regex_replacements_by_elem_path_regex.items():
            if regex == elem_path:
                # No regex was supplied in the settings dialog. The full path was given e.g.
                #   "top.core0.rob.stats.num_insts_retired"
                # 
                # Instead of something like:
                #   "top.core([0-9]+).rob.stats.num_insts_retired"
                #
                # We will just return the last part of the path as the caption using
                # heads-up camel case e.g. "NumInstsRetired[3]"
                return self.GetCaptionPrefix(elem_path) + '[{}]'.format(bin_idx)

            if re.compile(regex).match(elem_path):
                # This matched an elem path e.g.
                #   "top.core1.rob.stats.num_insts_retired"
                #
                # With a regex e.g.
                #   "top.core([0-9]+).rob.stats.num_insts_retired"
                #
                # We will return something like "NumInstsRetired1[3]"
                #                                               ^ ^
                #                                               | |
                #                                               | bin index
                #                                               core index
                return re.sub(regex, replacements, elem_path) + '[{}]'.format(bin_idx)

        return GetHeadsUpCamelCaseQueueName(elem_path) + '[{}]'.format(bin_idx)
    
    def GetCaptionPrefix(self, elem_path):
        for regex, replacements in self.regex_replacements_by_elem_path_regex.items():
            if regex == elem_path:
                return replacements

            if re.compile(regex).match(elem_path):
                return re.sub(regex, replacements, elem_path)

        return None

    def GetAllMatchingElemPaths(self):
        elem_paths = []
        for elem_path in self.simhier.GetContainerElemPaths():
            for regex, _ in self.regex_replacements_by_elem_path_regex.items():
                if elem_path == regex:
                    elem_paths.append(elem_path)
                    break

                if re.compile(regex).match(elem_path):
                    elem_paths.append(elem_path)
                    break

        return elem_paths

    def GetRegex(self, elem_path):
        for regex, _ in self.regex_replacements_by_elem_path_regex.items():
            if re.compile(regex).match(elem_path):
                return regex

        return None

    def GetMatchingElemPaths(self, regex):
        elem_paths = []
        for elem_path in self.simhier.GetContainerElemPaths():
            if re.compile(regex).match(elem_path):
                elem_paths.append(elem_path)

        return elem_paths

def GetHeadsUpCamelCaseQueueName(elem_path):
    parts = elem_path.split('.')
    queue_name = parts[-1]
    parts = queue_name.split('_')

    for i,part in enumerate(parts):
        if len(part) == 1:
            part = part.upper()
        else:
            part = part[0].upper() + part[1:]

        parts[i] = part

    return ''.join(parts)

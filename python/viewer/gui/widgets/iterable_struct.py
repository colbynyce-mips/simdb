import wx, wx.grid
from collections.abc import Iterable

class IterableStruct(wx.Panel):
    def __init__(self, parent, frame, elem_path):
        super(IterableStruct, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path
        self.deserializer = frame.data_retriever.GetDeserializer(elem_path)

        self._field_names = self.deserializer.GetFieldNames()
        self.capacity = frame.simhier.GetCapacityByElemPath(elem_path)
        self.grid = wx.grid.Grid(self)
        self.grid.CreateGrid(self.capacity, len(self._field_names), wx.grid.Grid.GridSelectNone)
        self.grid.EnableEditing(False)

        for i, field_name in enumerate(self._field_names):
            self.grid.SetColLabelValue(i, field_name)

        for i in range(self.capacity):
            self.grid.SetRowLabelValue(i, str(i))

        self.utiliz_elem = UtilizElement(self, frame, self.capacity)
        location_elem = wx.StaticText(self, label=elem_path)

        font = wx.Font(10, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)
        location_elem.SetFont(font)

        row1 = wx.BoxSizer(wx.HORIZONTAL)
        row1.Add(self.utiliz_elem, 0, wx.ALL, 5)
        row1.Add(location_elem, 1, wx.EXPAND | wx.ALL, 5)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(row1, 0, wx.EXPAND | wx.ALL, 10)
        sizer.Add(self.grid, 1, wx.EXPAND | wx.ALL, 10)
        self.SetSizer(sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'IterableStruct$' + self.elem_path

    def UpdateWidgetData(self):
        widget_renderer = self.frame.widget_renderer
        tick = widget_renderer.tick
        queue_data = self.frame.data_retriever.Unpack(self.elem_path, (tick,tick))

        self.__ClearGrid()

        if isinstance(queue_data['DataVals'][0], Iterable):
            for idx, row_data in enumerate(queue_data['DataVals'][0]):
                if row_data is None:
                    continue

                for j, field_name in enumerate(self._field_names):
                    self.grid.SetCellValue(idx, j, str(row_data[field_name]))
            num_rows_shown = len(queue_data['DataVals'][0])
        else:
            num_rows_shown = 0

        for i in range(num_rows_shown):
            self.grid.ShowRow(i)

        for i in range(num_rows_shown, self.capacity):
            self.grid.HideRow(i)

        self.utiliz_elem.UpdateUtilizPct(self.frame.widget_renderer.utiliz_handler.GetUtilizPct(self.elem_path))
        self.grid.AutoSizeColumns()
        self.Layout()

    def __ClearGrid(self):
        for i in range(self.grid.GetNumberRows()):
            for j in range(self.grid.GetNumberCols()):
                self.grid.SetCellValue(i, j, '')
                self.grid.SetCellBackgroundColour(i, j, wx.WHITE)

class UtilizElement(wx.StaticText):
    def __init__(self, parent, frame, capacity):
        super().__init__(parent)
        self.frame = frame
        self.capacity = capacity

        font = wx.Font(10, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)
        self.SetFont(font)

    def UpdateUtilizPct(self, utiliz_pct):
        self.SetLabel('{}%'.format(round(utiliz_pct * 100)))
        color = self.frame.widget_renderer.utiliz_handler.ConvertUtilizPctToColor(utiliz_pct)
        self.SetBackgroundColour(color)

        tooltip = 'Utilization: {}% ({}/{} bins filled)'.format(round(utiliz_pct * 100), int(utiliz_pct * self.capacity), self.capacity)
        self.SetToolTip(tooltip)
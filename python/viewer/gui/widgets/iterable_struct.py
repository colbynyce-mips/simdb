import wx, wx.grid

class IterableStruct(wx.Panel):
    @classmethod
    def CreateWidget(cls, parent, frame, elem_path):
        return cls(parent, frame, elem_path)

    def __init__(self, parent, frame, elem_path):
        super(IterableStruct, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path
        self.deserializer = frame.data_retriever.GetDeserializer(elem_path)

        field_names = self.deserializer.GetFieldNames()
        self.capacity = frame.widget_renderer.utiliz_handler.GetCapacity(elem_path)
        self.grid = wx.grid.Grid(self)
        self.grid.CreateGrid(self.capacity*20, len(field_names))

        for i, field_name in enumerate(field_names):
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
        sizer.Add(row1, 1, wx.EXPAND | wx.ALL, 10)
        sizer.Add(self.grid, 1, wx.EXPAND | wx.ALL, 10)
        self.SetSizer(sizer)
        self.Layout()

    def UpdateWidgetData(self):
        self.utiliz_elem.UpdateUtilizPct(self.frame.widget_renderer.utiliz_handler.GetUtilizPct(self.elem_path))
        self.grid.AutoSizeColumns()
        self.Layout()

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
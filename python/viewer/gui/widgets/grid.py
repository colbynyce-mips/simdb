import wx, wx.grid

class Grid(wx.grid.Grid):
    def __init__(self, parent, frame, rows, cols, cell_font=None, label_font=None, cell_selection_allowed=True, **kwargs):
        super().__init__(parent, **kwargs)
        self.frame = frame
        self.renderer = GridCellRenderer(rows, cols, cell_font)

        if label_font:
            self.SetLabelFont(label_font)

        self.CreateGrid(rows, cols)
        self.EnableEditing(False)
        self.DisableDragColMove()
        self.DisableDragColSize()
        self.DisableDragRowMove()
        self.DisableDragRowSize()
        self.DisableDragGridSize()
        self.DisableCellEditControl()

        if not cell_selection_allowed:
            self.Bind(wx.grid.EVT_GRID_SELECT_CELL, lambda evt: evt.Veto())

        self.SetDefaultRenderer(self.renderer)

    def SetCellValue(self, row, col, value, immediate_refresh=False):
        self.renderer.SetCellValue(row, col, value)
        if immediate_refresh:
            self.Refresh()
            self.AutoSize()

    def GetCellValue(self, row, col):
        return self.renderer.GetCellValue(row, col)
    
    def SetBackgroundColour(self, row, col, color, immediate_refresh=False):
        self.renderer.SetBackgroundColour(row, col, color)
        if immediate_refresh:
            self.Refresh()
            self.AutoSize()

    def GetBackgroundColour(self, row, col):
        return self.renderer.GetBackgroundColour(row, col)

    def SetCellBorder(self, row, col, border_width=1, border_side=wx.ALL, immediate_refresh=False):
        self.renderer.SetBorder(row, col, border_width, border_side)
        if immediate_refresh:
            self.Refresh()
            self.AutoSize()

    def ClearGrid(self):
        for row in range(self.GetNumberRows()):
            for col in range(self.GetNumberCols()):
                self.SetCellValue(row, col, '')
                self.SetBackgroundColour(row, col, (255, 255, 255))
                self.SetCellBorder(row, col, 0)

        self.Refresh()
        self.AutoSize()

class GridCellRenderer(wx.grid.GridCellRenderer):
    def __init__(self, rows, cols, font=None):
        super().__init__()
        self.cells = [[GridCell(font) for _ in range(cols)] for _ in range(rows)]

    def SetCellValue(self, row, col, value):
        self.cells[row][col].SetText(value)

    def GetCellValue(self, row, col):
        return self.cells[row][col].GetText()

    def SetBackgroundColour(self, row, col, color):
        self.cells[row][col].SetBackgroundColour(color)

    def GetBackgroundColour(self, row, col):
        return self.cells[row][col].GetBackgroundColour()

    def SetBorder(self, row, col, border_width, border_side):
        self.cells[row][col].SetBorder(border_width, border_side)

    def Draw(self, grid, attr, dc, rect, row, col, isSelected):
        self.cells[row][col].Draw(grid, dc, rect)

    def GetBestSize(self, grid, attr, dc, row, col):
        return self.cells[row][col].GetBestSize(grid, attr, dc, row, col)

class GridCell:
    def __init__(self, font=None):
        self.text = ''
        self.font = font if font else wx.Font(12, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL)
        self.background_colour = (255, 255, 255)
        self.border_width = 0
        self.border_side = wx.ALL

    def SetText(self, text):
        self.text = text

    def GetText(self):
        return self.text
    
    def SetBackgroundColour(self, color):
        self.background_colour = color

    def GetBackgroundColour(self):
        return self.background_colour
    
    def SetBorder(self, border_width, border_side):
        self.border_width = border_width
        self.border_side = border_side

    def Draw(self, grid, dc, rect):
        dc.SetBrush(wx.Brush(self.background_colour))
        dc.SetPen(wx.Pen(wx.TRANSPARENT_PEN))
        dc.DrawRectangle(rect)

        if self.text:
            dc.SetFont(self.font)
            dc.DrawLabel(self.text, rect, wx.ALIGN_CENTER)

        if self.border_width:
            dc.SetPen(wx.Pen(wx.BLACK, self.border_width))
            if self.border_side & wx.TOP:
                dc.DrawLine(rect.GetLeft(), rect.GetTop(), rect.GetRight(), rect.GetTop())
            if self.border_side & wx.BOTTOM:
                dc.DrawLine(rect.GetLeft(), rect.GetBottom(), rect.GetRight(), rect.GetBottom())
            if self.border_side & wx.LEFT:
                dc.DrawLine(rect.GetLeft(), rect.GetTop(), rect.GetLeft(), rect.GetBottom())
            if self.border_side & wx.RIGHT:
                dc.DrawLine(rect.GetRight(), rect.GetTop(), rect.GetRight(), rect.GetBottom())

    def GetBestSize(self, grid, attr, dc, row, col):
        if not self.text:
            return (dc.GetTextExtent(' ')[0], 5)

        dc.SetFont(self.font)
        w,h = dc.GetTextExtent(self.text)
        w += 2
        h += 2
        return wx.Size(w, h)

import wx

class CanvasGrid(wx.Panel):
    def __init__(self, inspector):
        super(CanvasGrid, self).__init__(inspector)
        self.frame = inspector.frame

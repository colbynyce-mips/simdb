import wx

class ScalarStatistic(wx.Panel):
    @classmethod
    def CreateWidget(cls, parent, frame, elem_path):
        return cls(parent, frame, elem_path)

    def __init__(self, parent, frame, elem_path):
        super(ScalarStatistic, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path

        self.SetBackgroundColour('green')
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(self.sizer)
        self.Layout()
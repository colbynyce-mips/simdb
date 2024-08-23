import wx

class IterableStruct(wx.Panel):
    @classmethod
    def CreateWidget(cls, parent, frame, elem_path):
        return cls(parent, frame, elem_path)

    def __init__(self, parent, frame, elem_path):
        super(IterableStruct, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path
        
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Iterable Struct:\n%s' % elem_path), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def UpdateWidgetData(self):
        pass
import wx

class InspectorTab(wx.Panel):
    def __init__(self, inspector):
        super(InspectorTab, self).__init__(inspector)
        self.frame = inspector.frame

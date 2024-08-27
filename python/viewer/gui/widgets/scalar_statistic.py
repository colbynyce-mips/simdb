import wx

class ScalarStatistic(wx.Panel):
    def __init__(self, parent, frame, elem_path):
        super(ScalarStatistic, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(wx.StaticText(self, label='Scalar Statistic:\n%s' % elem_path), 0, wx.EXPAND)
        self.SetSizer(self.sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'ScalarStatistic$' + self.elem_path

    def UpdateWidgetData(self):
        widget_renderer = self.frame.widget_renderer
        print ("QueueUtilizWidget UpdateWidgetData called at tick %d" % widget_renderer.tick)
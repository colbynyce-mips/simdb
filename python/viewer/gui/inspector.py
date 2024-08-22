import wx

class DataInspector(wx.Notebook):
    def __init__(self, parent, frame):
        super(DataInspector, self).__init__(parent, style=wx.NB_TOP)
        self.frame = frame

        # Add tabs to notebook1
        self.AddPage(wx.Panel(self), "BAR")
        self.AddPage(wx.Panel(self), "BIZ")
        self.AddPage(wx.Panel(self), "BUZ")
        self.SetMinSize((200, 200))

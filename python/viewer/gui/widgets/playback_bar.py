import wx

class PlaybackBar(wx.Panel):
    def __init__(self, frame):
        super(PlaybackBar, self).__init__(frame, size=(frame.GetSize().width, 100))
        self.SetBackgroundColour('light gray')

    @property
    def frame(self):
        return self.GetParent()

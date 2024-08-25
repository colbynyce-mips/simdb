import wx
from viewer.gui.canvas_grid import WidgetContainer

class HyperlinkLocation(wx.StaticText):
    def __init__(self, parent, frame, location):
        super().__init__(parent, label=location)
        self.frame = frame
        self.location = location
        self.navtree = frame.explorer.navtree

        self.widget_name = self.navtree.GetWidgetName(location)
        assert self.widget_name

        widget_container = parent
        while widget_container and not isinstance(widget_container, WidgetContainer):
            widget_container = widget_container.GetParent()

        assert widget_container
        self.widget_container = widget_container

        self.SetForegroundColour(wx.Colour(0, 0, 255))
        self.Bind(wx.EVT_LEFT_DOWN, self.__ShowWidget)

    def __ShowWidget(self, event):
        widget = self.navtree.CreateWidget(self.widget_name, self.location, self.widget_container)
        self.widget_container.SetWidget(widget)
        widget.UpdateWidgetData()

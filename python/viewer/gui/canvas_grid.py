import wx

class CanvasGrid(wx.Panel):
    def __init__(self, parent, rows=1, cols=1):
        super(CanvasGrid, self).__init__(parent)

        if rows == 1 and cols == 1:
            self.container = WidgetContainer(self)
            self.Bind(wx.EVT_CONTEXT_MENU, self.__OnContextMenu)
        else:
            self.container = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
            self.container.SetMinimumPaneSize(300)
            self.__BuildGrid(self.container, rows, cols)
            if not self.container.GetSizer():
                sizer = wx.BoxSizer(wx.VERTICAL)
                sizer.Add(self.container.GetWindow1(), 1, wx.EXPAND)
                sizer.Add(self.container.GetWindow2(), 1, wx.EXPAND)
                self.container.SetSizer(sizer)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.container, 1, wx.EXPAND)
        self.SetSizer(sizer)

    @property
    def frame(self):
        frame = self.GetParent()
        while frame and not isinstance(frame, wx.Frame):
            frame = frame.GetParent()

        return frame
    
    def DestroyAllWidgets(self):
        if isinstance(self.container, WidgetContainer):
            self.container.DestroyAllWidgets()
        else:
            for child in self.container.GetChildren():
                if isinstance(child, CanvasGrid):
                    child.DestroyAllWidgets()

    def UpdateWidgets(self):
        if isinstance(self.container, WidgetContainer):
            self.container.UpdateWidgets()
        else:
            for child in self.container.GetChildren():
                if isinstance(child, CanvasGrid):
                    child.UpdateWidgets()

    def __BuildGrid(self, splitter, rows, cols):
        assert rows > 0 and cols > 0

        if rows > 1 and cols > 1:
            top_splitter = wx.SplitterWindow(splitter, style=wx.SP_LIVE_UPDATE)
            bottom_splitter = wx.SplitterWindow(splitter, style=wx.SP_LIVE_UPDATE)
            top_splitter.SetMinimumPaneSize(300)
            bottom_splitter.SetMinimumPaneSize(300)

            top_grid = CanvasGrid(top_splitter, rows=rows // 2, cols=cols)
            top_sizer = wx.BoxSizer(wx.VERTICAL)
            top_sizer.Add(top_grid, 1, wx.EXPAND)
            top_splitter.SetSizer(top_sizer)

            bottom_grid = CanvasGrid(bottom_splitter, rows=rows - rows // 2, cols=cols)
            bottom_sizer = wx.BoxSizer(wx.VERTICAL)
            bottom_sizer.Add(bottom_grid, 1, wx.EXPAND)
            bottom_splitter.SetSizer(bottom_sizer)

            splitter.SplitHorizontally(top_splitter, bottom_splitter)

            sizer = wx.BoxSizer(wx.VERTICAL)
            sizer.Add(top_splitter, 1, wx.EXPAND)
            sizer.Add(bottom_splitter, 1, wx.EXPAND)
            splitter.SetSizer(sizer)
        elif rows == 1:
            splitter.SplitVertically(CanvasGrid(splitter, rows, cols // 2), CanvasGrid(splitter, rows, cols - cols // 2))
        elif cols == 1:
            splitter.SplitHorizontally(CanvasGrid(splitter, rows // 2, cols), CanvasGrid(splitter, rows - rows // 2, cols))

    def __OnContextMenu(self, event):
        # Get the position where the user right-clicked
        pos = event.GetPosition()
        pos = self.ScreenToClient(pos)

        menu = wx.Menu()

        split_vertically = menu.Append(-1, "Split left/right")
        split_horizontally = menu.Append(-1, "Split top/bottom")
        split_custom = menu.Append(-1, "Split custom")

        self.Bind(wx.EVT_MENU, self.__OnSplitVertically, split_vertically)
        self.Bind(wx.EVT_MENU, self.__OnSplitHorizontally, split_horizontally)
        self.Bind(wx.EVT_MENU, self.__OnSplitCustom, split_custom)

        self.PopupMenu(menu, pos)

    def __OnSplitVertically(self, event):
        if self.container:
            self.container.DestroyAllWidgets()

        self.GetSizer().Clear()
        self.container = CanvasGrid(self, rows=1, cols=2)
        self.GetSizer().Add(self.container, 1, wx.EXPAND)
        self.Layout()

    def __OnSplitHorizontally(self, event):
        if self.container:
            self.container.DestroyAllWidgets()

        self.GetSizer().Clear()
        self.container = CanvasGrid(self, rows=2, cols=1)
        self.GetSizer().Add(self.container, 1, wx.EXPAND)
        self.Layout()

    def __OnSplitCustom(self, event):
        dlg = wx.TextEntryDialog(self, "Enter number of rows and columns separated by a comma:", "Custom Split", value="2,2")

        if dlg.ShowModal() == wx.ID_OK:
            rows, cols = dlg.GetValue().strip().split(',')
            rows = int(rows)
            cols = int(cols)

            if self.container:
                self.container.DestroyAllWidgets()

            self.GetSizer().Clear()
            self.container = CanvasGrid(self, rows=rows, cols=cols)
            self.GetSizer().Add(self.container, 1, wx.EXPAND)
            self.Layout()

        dlg.Destroy()

class WidgetContainer(wx.Panel):
    def __init__(self, parent):
        super(WidgetContainer, self).__init__(parent)
        self._widget = None

        frame = parent 
        while frame and not isinstance(frame, wx.Frame):
            frame = frame.GetParent()

        self.SetDropTarget(WidgetContainerDropTarget(self, frame.explorer.navtree))

        sizer = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(sizer)

    @property
    def frame(self):
        frame = self.GetParent()
        while frame and not isinstance(frame, wx.Frame):
            frame = frame.GetParent()

        return frame
    
    def SetWidget(self, widget):
        if self._widget:
            self.GetSizer().Detach(self._widget)
            self._widget.Destroy()

        self._widget = widget
        sizer = self.GetSizer()
        sizer.Add(widget, 1, wx.EXPAND)
        self.Layout()

    def SetWidgetFocus(self):
        if self._widget:
            self._widget.SetFocus()

    def DestroyAllWidgets(self):
        if self._widget:
            self.GetSizer().Detach(self._widget)
            self._widget.Destroy()
            self._widget = None

    def UpdateWidgets(self):
        if self._widget:
            self._widget.UpdateWidgetData()

class WidgetContainerDropTarget(wx.TextDropTarget):
    def __init__(self, widget_container, navtree):
        super(WidgetContainerDropTarget, self).__init__()
        self.widget_container = widget_container
        self.navtree = navtree

    def OnDropText(self, x, y, text):
        return self.__CreateWidget(text) or self.__CreateTool(text)

    def __CreateWidget(self, text):
        if text.find('$') == -1:
            return False

        factory_name, elem_path = text.split('$')
        widget = self.navtree.CreateWidget(factory_name, elem_path, self.widget_container)
        if not widget:
            return False
        
        self.widget_container.SetWidget(widget)
        wx.CallAfter(self.__RenderWidget)
        return True
        
    def __CreateTool(self, tool_name):
        tool = self.navtree.GetTool(tool_name)
        if not tool:
            return False
        
        widget = tool.CreateWidget(self.widget_container, self.widget_container.frame)
        if not widget:
            return False
        
        self.widget_container.SetWidget(widget)
        wx.CallAfter(self.__RenderWidget)
        return True

    def __RenderWidget(self):
        self.widget_container.UpdateWidgets()
        self.widget_container.SetWidgetFocus()

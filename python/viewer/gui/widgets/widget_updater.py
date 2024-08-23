class WidgetUpdater:
    def __init__(self, frame):
        self.frame = frame
        cursor = frame.db.cursor
        cursor.execute('SELECT MIN(TimeVal), MAX(TimeVal) FROM CollectionData')
        self._start_tick, self._end_tick = cursor.fetchone()
        self._current_tick = self._start_tick

    def GetCurTick(self):
        return self._current_tick
    
    def GetStartTick(self):
        return self._start_tick
    
    def GetEndTick(self):
        return self._end_tick
    
    def GoToTick(self, tick):
        tick = min(max(tick, self._start_tick), self._end_tick)
        self._current_tick = tick
        self.frame.playback_bar.SyncControls(tick)
        self.__UpdateWidgets()

    def GoToStart(self):
        self.GoToTick(self._start_tick)

    def GoToEnd(self):
        self.GoToTick(self._end_tick)

    def __UpdateWidgets(self):
        self.frame.explorer.navtree.UpdateUtilizBitmaps()
        self.frame.explorer.watchlist.UpdateUtilizBitmaps()

        notebook = self.frame.inspector
        page_idx = notebook.GetSelection()
        page = notebook.GetPage(page_idx)
        page.UpdateWidgets()

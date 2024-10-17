import wx

class WidgetRenderer:
    def __init__(self, frame):
        self.frame = frame
        cursor = frame.db.cursor()
        cursor.execute('SELECT MIN(TimeVal), MAX(TimeVal) FROM CollectionData')
        self._start_tick, self._end_tick = cursor.fetchone()
        self._current_tick = self._start_tick
        self._utiliz_handler = IterableUtiliz(self)

    @property
    def tick(self):
        return self._current_tick

    @property
    def start_tick(self):
        return self._start_tick

    @property
    def end_tick(self):
        return self._end_tick
    
    @property
    def utiliz_handler(self):
        return self._utiliz_handler
    
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

class IterableUtiliz:
    def __init__(self, widget_renderer):
        self.widget_renderer = widget_renderer
        self._utiliz_pcts_by_sim_path = {}

        cursor = self.widget_renderer.frame.db.cursor()

        cursor.execute('SELECT PathID,Capacity FROM ContainerMeta')
        self._capacities_by_path_id = {}
        for path_id,capacity in cursor.fetchall():
            self._capacities_by_path_id[path_id] = capacity

        cursor.execute('SELECT Id,CollectionID,SimPath FROM CollectionElems')
        self._capacities_by_sim_path = {}
        self._capacities_by_collection_id = {}
        self._collection_ids_by_sim_path = {}
        self._all_sim_paths = []
        for path_id,collection_id,simpath in cursor.fetchall():
            if path_id not in self._capacities_by_path_id:
                continue

            self._capacities_by_sim_path[simpath] = self._capacities_by_path_id[path_id]
            self._capacities_by_collection_id[collection_id] = self._capacities_by_path_id[path_id]
            self._collection_ids_by_sim_path[simpath] = collection_id
            self._all_sim_paths.append(simpath)

    def GetCapacity(self, sim_path):
        return self._capacities_by_sim_path[sim_path]

    def GetUtilizPct(self, sim_path):
        self.__CacheUtilizValues()
        return self._utiliz_pcts_by_sim_path.get(sim_path, 0)

    def GetUtilizColor(self, sim_path):
        self.__CacheUtilizValues()
        utiliz_pct = self._utiliz_pcts_by_sim_path.get(sim_path, 0)
        return self.__GetColorForUtilizPct(utiliz_pct)
    
    def ConvertUtilizPctToColor(self, utiliz_pct):
        return self.__GetColorForUtilizPct(utiliz_pct)

    def CreateUtilizImageList(self):
        image_list = wx.ImageList(16, 16)
        for utiliz_pct in range(0, 101):
            utiliz_color = self.__GetColorForUtilizPct(utiliz_pct / 100)
            utiliz_bitmap = self.__CreateBitmap(utiliz_color)
            image_list.Add(utiliz_bitmap)

        return image_list

    def __CacheUtilizValues(self):
        # We need to get the number of data elements in each simpath blob at this time step.
        data_retriever = self.widget_renderer.frame.data_retriever
        queue_sizes_by_collection_id = data_retriever.GetIterableSizesByCollectionID(self.widget_renderer.tick)

        self._utiliz_pcts_by_sim_path = {}
        for sim_path in self._all_sim_paths:
            collection_id = self._collection_ids_by_sim_path[sim_path]
            self._utiliz_pcts_by_sim_path[sim_path] = queue_sizes_by_collection_id[collection_id] / self._capacities_by_collection_id[collection_id]

    def __GetColorForUtilizPct(self, utiliz_pct):
        """
        Maps a floating point value from 0 to 1 to an RGB color for a heatmap.
        The gradient transitions from white to red.
        
        Args:
        - utiliz_pct (float): A floating point value between 0 and 1.
        
        Returns:
        - tuple: An (R, G, B) color value where each component is in the range [0, 255].
        """

        # Cap the input value to [0,1]
        utiliz_pct = max(0, min(1, utiliz_pct))

        # Define the color transition: White (low) to Red (high)
        # Interpolating between white (255, 255, 255) and red (255, 0, 0)
        
        # White color components
        white = (255, 255, 255)
        # Red color components
        red = (255, 0, 0)
        
        # Interpolate between white and red based on the value
        r = int(white[0] * (1 - utiliz_pct) + red[0] * utiliz_pct)
        g = int(white[1] * (1 - utiliz_pct) + red[1] * utiliz_pct)
        b = int(white[2] * (1 - utiliz_pct) + red[2] * utiliz_pct)
        
        return (r, g, b)

    def __CreateBitmap(self, utiliz_color, size=(16, 16)):
        """
        Creates a bitmap from an RGB color.
        
        Args:
        - utiliz_color (tuple): An (R, G, B) color value where each component is in the range [0, 255].
        - size (tuple): Size of the bitmap (width, height).
        
        Returns:
        - wx.Bitmap: A bitmap representing the specified color.
        """
        width, height = size
        image = wx.Image(width, height)
        image.SetRGB(wx.Rect(0, 0, width, height), *utiliz_color)
        return wx.Bitmap(image)

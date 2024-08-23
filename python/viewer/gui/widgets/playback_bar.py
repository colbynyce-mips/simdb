import wx
from functools import partial

class PlaybackBar(wx.Panel):
    def __init__(self, frame):
        super(PlaybackBar, self).__init__(frame, size=(frame.GetSize().width, 100))
        self.SetBackgroundColour('light gray')

        # create a combobox for selecting the clock, with default value '<any clk edge>'
        self.clock_combobox = wx.ComboBox(self, choices=['<any clk edge>'], value='<any clk edge>', style=wx.CB_READONLY)

        # create a statictext for displaying the current cycle (make it bold with the string 'cycle:1')
        self.current_cyc_text = wx.StaticText(self, label='cycle:1')
        font = self.current_cyc_text.GetFont()
        font.SetWeight(wx.FONTWEIGHT_BOLD)
        self.current_cyc_text.SetFont(font)

        # create a statictext for displaying the current tick (make it regular font with the string 'tick:1')
        self.current_tick_text = wx.StaticText(self, label='tick:1')

        # create buttons with the labels '-30', '-10', '-3', '-1', '+1', '+3', '+10', '+30'
        self.minus_30_button = wx.Button(self, label='-30')
        self.minus_10_button = wx.Button(self, label='-10')
        self.minus_3_button = wx.Button(self, label='-3')
        self.minus_1_button = wx.Button(self, label='-1')
        self.plus_1_button = wx.Button(self, label='+1')
        self.plus_3_button = wx.Button(self, label='+3')
        self.plus_10_button = wx.Button(self, label='+10')
        self.plus_30_button = wx.Button(self, label='+30')

        # make the buttons the same size (40x25)
        for button in [self.minus_30_button, self.minus_10_button, self.minus_3_button, self.minus_1_button,
                       self.plus_1_button, self.plus_3_button, self.plus_10_button, self.plus_30_button]:
            button.SetSize((40, 25))

        # bind the step forward/backward buttons to the corresponding functions
        self.minus_30_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=-30))
        self.minus_10_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=-10))
        self.minus_3_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=-3))
        self.minus_1_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=-1))
        self.plus_1_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=1))
        self.plus_3_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=3))
        self.plus_10_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=10))
        self.plus_30_button.Bind(wx.EVT_BUTTON, partial(self.__OnStep, step=30))

        # add a hyperlinked statictext labeled 'cyc-start:1'
        self.cyc_start_text = wx.StaticText(self, label='cyc-start:1')
        self.cyc_start_text.SetForegroundColour(wx.BLUE)
        self.cyc_start_text.Bind(wx.EVT_LEFT_DOWN, self.__OnCycStart)

        # add a hyperlinked statictext labeled 'cyc-end:100'
        self.cyc_end_text = wx.StaticText(self, label='cyc-end:100')
        self.cyc_end_text.SetForegroundColour(wx.BLUE)
        self.cyc_end_text.Bind(wx.EVT_LEFT_DOWN, self.__OnCycEnd)

        # add a slider with range 1-100
        self.cyc_slider = wx.Slider(self, minValue=1, maxValue=100, style=wx.SL_HORIZONTAL)

        curticks = wx.BoxSizer(wx.VERTICAL)
        curticks.Add(self.current_cyc_text, 1, wx.EXPAND)
        curticks.Add(self.current_tick_text, 1, wx.EXPAND)

        #row1 = wx.FlexGridSizer(cols=2)
        #row1.AddGrowableCol(0)
        #row1.AddGrowableCol(1)
        #row1.AddGrowableRow(0)
        row1 = wx.BoxSizer(wx.HORIZONTAL)

        #clock_sizer = wx.FlexGridSizer(cols=2)
        #clock_sizer.AddGrowableRow(0)
        #clock_sizer.Add(self.clock_combobox, 0, wx.ALIGN_CENTER_VERTICAL | wx.SHAPED)
        #clock_sizer.Add(curticks, 0, wx.EXPAND | wx.LEFT, 3)
        #row1.Add(clock_sizer, 0, wx.ALIGN_CENTER_VERTICAL | wx.EXPAND)
        row1.Add(self.clock_combobox, 0, wx.EXPAND)
        row1.Add(curticks, 0, wx.EXPAND)
        row1.AddStretchSpacer(1)

        #nav_sizer = wx.FlexGridSizer(cols=8)
        #nav_sizer.AddGrowableRow(0)
        nav_sizer = wx.BoxSizer(wx.HORIZONTAL)
        nav_sizer.Add(self.minus_30_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.minus_10_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.minus_3_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.minus_1_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.plus_1_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.plus_3_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.plus_10_button, 0, wx.ALIGN_CENTER_VERTICAL)
        nav_sizer.Add(self.plus_30_button, 0, wx.ALIGN_CENTER_VERTICAL)
        row1.Add(nav_sizer)
        row1.AddStretchSpacer(1)

        row2 = wx.BoxSizer(wx.HORIZONTAL)
        row2.Add(self.cyc_start_text, 0, wx.ALIGN_CENTER_VERTICAL | wx.LEFT | wx.RIGHT, 2)
        slider_sizer = wx.BoxSizer(wx.HORIZONTAL)
        slider_sizer.Add(self.cyc_slider, 1, wx.ALIGN_CENTER_VERTICAL)
        row2.Add(slider_sizer, 1, wx.EXPAND)
        row2.Add(self.cyc_end_text, 0, wx.ALIGN_CENTER_VERTICAL | wx.LEFT | wx.RIGHT, 2)

        rows = wx.BoxSizer(wx.VERTICAL)
        rows.Add(row1, 0, wx.EXPAND | wx.TOP, 2)
        rows.Add(wx.StaticLine(self, wx.ID_ANY, style=wx.VERTICAL), 0, wx.EXPAND | wx.TOP | wx.BOTTOM, 4)
        rows.Add(row2, 0, wx.EXPAND)

        self.SetSizer(rows)
        self.Fit()
        self.SetAutoLayout(True)

    def __OnStep(self, event, step):
        pass

    def __OnCycStart(self, event):
        pass

    def __OnCycEnd(self, event):
        pass

    @property
    def frame(self):
        return self.GetParent()

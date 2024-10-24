import wx
import pylab as pl
import matplotlib
import numpy as np
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg as FigureCanvas

class ScalarStatistic(wx.Panel):
    def __init__(self, parent, frame, elem_path):
        super(ScalarStatistic, self).__init__(parent)
        self.frame = frame
        self.elem_path = elem_path

        sizer = wx.BoxSizer(wx.HORIZONTAL)
        stat_values = frame.data_retriever.Unpack(elem_path)

        # Create a timeseries plot
        if len(stat_values['TimeVals']) > 0:
            self.time_vals = stat_values['TimeVals']
            self.data_vals = stat_values['DataVals']
            self.figure  = matplotlib.figure.Figure()
            self.canvas = FigureCanvas(self, -1, self.figure)
            self.canvas.SetPosition((25,-10))

            self.ax = self.figure.add_subplot(111)
            self.ax.plot(self.time_vals, self.data_vals, 'b-')
            self.ax.set_title(self.elem_path)
            self.ax.set_xlabel('Ticks')
            self.ax.set_ylabel('Values')
            self.ax.grid()
            self.ax.autoscale()

            # Add a ear button (size 16x16) to the left of the time series plot.
            # Clicking the button will open a dialog to change the plot settings.
            # Note that we do not add the button to the sizer since we want to
            # force it to be in the top-left corner of the widget canvas. We do
            # this with the 'pos' argument to the wx.BitmapButton constructor.
            gear_btn = wx.BitmapButton(self, bitmap=frame.CreateResourceBitmap('gear.png'), pos=(5,5))
            gear_btn.Bind(wx.EVT_BUTTON, self.__EditWidget)
            gear_btn.SetToolTip('Edit widget settings')
        else:
            sizer.Add(wx.StaticText(self, label='No data for stat at location:\n%s' % elem_path), 0, wx.EXPAND)

        self.SetSizer(sizer)
        self.Layout()

    def GetWidgetCreationString(self):
        return 'ScalarStatistic$' + self.elem_path

    def UpdateWidgetData(self):
        # Nothing to do since we plot all data
        pass

    def GetCurrentViewSettings(self):
        settings = {}
        settings['title'] = self.ax.get_title()
        settings['xlabel'] = self.ax.xaxis.get_label().get_text()
        settings['ylabel'] = self.ax.yaxis.get_label().get_text()
        settings['show_xlabel'] = self.ax.xaxis.get_label().get_visible()
        settings['show_ylabel'] = self.ax.yaxis.get_label().get_visible()
        return settings
    
    def GetCurrentUserSettings(self):
        return {}

    def ApplyViewSettings(self, settings):
        if settings == self.GetCurrentViewSettings():
            return

        self.ax.set_title(settings['title'])
        self.ax.set_xlabel(settings['xlabel'])
        self.ax.set_ylabel(settings['ylabel'])
        self.ax.xaxis.get_label().set_visible(settings['show_xlabel'])
        self.ax.yaxis.get_label().set_visible(settings['show_ylabel'])
        self.canvas.draw()
        self.Update()
        self.Refresh()
        self.frame.view_settings.dirty = True

    def __EditWidget(self, event):
        dlg = PlotCustomizationDialog(self, **self.GetCurrentViewSettings())
        if dlg.ShowModal() == wx.ID_OK:
            self.ApplyViewSettings(dlg.GetSettings())

        dlg.Destroy()

class PlotCustomizationDialog(wx.Dialog):
    def __init__(self, parent, title='Timeseries', xlabel='Time', ylabel='Values', show_xlabel=True, show_ylabel=True):
        super().__init__(parent, title="Customize Plot", size=(500, 600))

        # Create the main sizer
        sizer = wx.BoxSizer(wx.VERTICAL)

        # Title
        self.title_text = wx.TextCtrl(self, value=title)
        sizer.Add(wx.StaticText(self, label="Title:"), 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.title_text, 0, wx.ALL | wx.EXPAND, 5)

        # X-axis Label
        self.x_label_text = wx.TextCtrl(self, value=xlabel)
        sizer.Add(wx.StaticText(self, label="X-axis Label:"), 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.x_label_text, 0, wx.ALL | wx.EXPAND, 5)

        # Y-axis Label
        self.y_label_text = wx.TextCtrl(self, value=ylabel)
        sizer.Add(wx.StaticText(self, label="Y-axis Label:"), 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(self.y_label_text, 0, wx.ALL | wx.EXPAND, 5)

        # Checkboxes for showing labels
        self.show_x_label_checkbox = wx.CheckBox(self, label="Show X-axis Label")
        self.show_x_label_checkbox.SetValue(show_xlabel)
        sizer.Add(self.show_x_label_checkbox, 0, wx.ALL | wx.EXPAND, 5)

        self.show_y_label_checkbox = wx.CheckBox(self, label="Show Y-axis Label")
        self.show_y_label_checkbox.SetValue(show_ylabel)
        sizer.Add(self.show_y_label_checkbox, 0, wx.ALL | wx.EXPAND, 5)

        # OK and Cancel buttons
        btn_sizer = wx.StdDialogButtonSizer()
        btn_sizer.AddButton(wx.Button(self, wx.ID_OK))
        btn_sizer.AddButton(wx.Button(self, wx.ID_CANCEL))
        btn_sizer.Realize()

        sizer.Add(btn_sizer, 0, wx.ALIGN_CENTER | wx.ALL, 10)
        self.SetSizer(sizer)

    def GetSettings(self):
        return {
            'title': self.title_text.GetValue(),
            'xlabel': self.x_label_text.GetValue(),
            'ylabel': self.y_label_text.GetValue(),
            'show_xlabel': self.show_x_label_checkbox.GetValue(),
            'show_ylabel': self.show_y_label_checkbox.GetValue(),
        }

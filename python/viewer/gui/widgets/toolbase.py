import wx
from abc import abstractmethod

class ToolBase:
    def __init__(self):
        pass

    @abstractmethod
    def GetToolName(self):
        pass

    @abstractmethod
    def GetToolSettings(self):
        pass

    @abstractmethod
    def SetToolSettings(self, settings):
        pass

    @abstractmethod
    def CreateToolWidget(self, parent, frame):
        pass

    @abstractmethod
    def GetToolHelpText(self):
        return None
    
    @abstractmethod
    def ShowHelpDialog(self, help_text):
        pass

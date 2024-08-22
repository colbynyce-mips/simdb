import wx, sys
from viewer.model.workspace import Workspace

class MyApp(wx.App):
    def OnInit(self):
        return True

if __name__ == "__main__":
    app = MyApp()
    workspace = Workspace(sys.argv[1])
    app.MainLoop()
    workspace.Cleanup()

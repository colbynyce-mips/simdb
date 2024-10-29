import wx, sys, argparse
from viewer.model.workspace import Workspace

class MyApp(wx.App):
    def OnInit(self):
        return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, help="Path to the database file")
    parser.add_argument("--views-dir", help="Path to the directory containing the view files (*.avf)")
    parser.add_argument("--view-file", help="Path to the view file (*.avf) to load")
    args = parser.parse_args()

    app = MyApp()
    workspace = Workspace(args.database, args.views_dir, args.view_file)
    app.MainLoop()

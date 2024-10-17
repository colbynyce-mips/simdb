import wx, sys, argparse
from viewer.model.workspace import Workspace

class MyApp(wx.App):
    def OnInit(self):
        return True

if __name__ == "__main__":
    # create parser with --database (file path) and --views-dir (directory path) arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, help="Path to the database file")
    parser.add_argument("--views-dir", help="Path to the directory containing the view files (*.yaml)")
    args = parser.parse_args()

    app = MyApp()
    workspace = Workspace(args.database, args.views_dir)
    app.MainLoop()
    workspace.Cleanup()

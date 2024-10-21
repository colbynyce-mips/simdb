import wx, sys, argparse
from viewer.model.workspace import Workspace

class MyApp(wx.App):
    def OnInit(self):
        return True

if __name__ == "__main__":
<<<<<<< Updated upstream
    # create parser with --database (file path) and --views-dir (directory path) arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, help="Path to the database file")
    parser.add_argument("--views-dir", help="Path to the directory containing the view files (*.yaml)")
    args = parser.parse_args()

    app = MyApp()
    workspace = Workspace(args.database, args.views_dir)
=======
<<<<<<< Updated upstream
    app = MyApp()
    workspace = Workspace(sys.argv[1])
=======
    # create parser with --database (file path) and --views-dir (directory path) arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, help="Path to the database file")
    parser.add_argument("--views-dir", help="Path to the directory containing the view files (*.yaml)")
    parser.add_argument("--view-file", help="Path to the view file (*.avf) to load")
    args = parser.parse_args()

    app = MyApp()
    workspace = Workspace(args.database, args.views_dir, args.view_file)
>>>>>>> Stashed changes
>>>>>>> Stashed changes
    app.MainLoop()

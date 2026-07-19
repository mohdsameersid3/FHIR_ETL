
class FileUtils:

    def __init__(self, dbutils):
        self.dbutils = dbutils

    def list_json_files(self, folder):

        json_files = []

        def walk(path):
            for item in self.dbutils.fs.ls(path):
                if item.isDir():
                    walk(item.path)
                elif item.path.endswith(".json"):
                    json_files.append(item.path)

        walk(folder)

        return sorted(json_files)
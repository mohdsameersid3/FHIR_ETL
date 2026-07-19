import json
class FileSystem:

    def __init__(self, dbutils):
        self.dbutils = dbutils

    def mkdir(self, path):
        self.dbutils.fs.mkdirs(path)

    def write_json(self, path, obj):
        self.dbutils.fs.put(
            path,
            json.dumps(obj, indent=2),
            overwrite=True
        )

    def read_json(self, path):
        return json.loads(
            self.dbutils.fs.head(path, 1024 * 1024)
        )
        

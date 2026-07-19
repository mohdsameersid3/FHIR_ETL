# from pathlib import PurePosixPath, Path

# notebook_path = (
#     dbutils.notebook.entry_point
#     .getDbutils()
#     .notebook()
#     .getContext()
#     .notebookPath()
#     .get()
# )

# workspace_root = str(PurePosixPath(notebook_path).parent)
# dbutils.fs.ls("/Volumes/workspace/default/FHIR")
# base_path = "/Volumes/workspace/default/FHIR"

# folders = [
#     "raw",
#     "checkpoints",
#     "bronze",
#     "silver",
#     "gold"
# ]

# for folder in folders:
#     dbutils.fs.mkdirs(f"{base_path}/{folder}")

print(dbutils.fs.ls("/Volumes/workspace/default/FHIR/raw"))
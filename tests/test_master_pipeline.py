from src.pipeline.master_pipeline import MasterPipeline
from src.common.file_system import FileSystem
from src.common.spark import SparkSessionManager

dbutils = FileSystem(dbutils)
spark = SparkSessionManager().get_session()

pipeline = MasterPipeline(dbutils, spark)

pipeline.run()
from src.pipeline.master_pipeline import MasterPipeline
from src.common.file_system import FileSystem
from src.common.spark import SparkSessionManager

dbutils = FileSystem(dbutils)
spark = SparkSessionManager().get_session()

# Clean up existing tables to avoid schema mismatch
resources = ["patient", "encounter", "observation", "condition"]
for resource in resources:
    spark.sql(f"DROP TABLE IF EXISTS workspace.bronze.{resource}")
    spark.sql(f"DROP TABLE IF EXISTS workspace.silver.{resource}")
spark.sql("DROP TABLE IF EXISTS workspace.gold.gold_patient_summary")

pipeline = MasterPipeline(dbutils, spark)

pipeline.run()

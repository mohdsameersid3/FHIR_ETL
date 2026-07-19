from pyspark.sql.functions import col
from config.settings import settings


class SilverLoader:

    def __init__(self, spark):
        self.spark = spark
        self.resource_mappings = settings.get()["resource_mappings"]

    def load(self, resource):

        bronze_df = self.spark.read.table(
            f"workspace.bronze.{resource.lower()}"
        )

        metadata_columns = [
            col("row_hash"),
            col("api_url"),
            col("load_timestamp")
        ]

        mapped_columns = [
            col(source_path).alias(target_name)
            for target_name, source_path
            in self.resource_mappings[resource]["columns"].items()
        ]

        silver_df = bronze_df.select(
            *metadata_columns,
            *mapped_columns
        )

        return silver_df
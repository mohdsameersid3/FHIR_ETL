from pyspark.sql.functions import expr, coalesce, lit, col
from config.settings import settings
from src.common.logger import LoggerFactory


class SilverLoader:

    def __init__(self, spark):
        self.spark = spark
        self.resource_mappings = settings.get()["resource_mappings"]
        self.logger = LoggerFactory().get_logger(__name__)

    def load(self, resource):

        bronze_df = self.spark.read.table(
            f"workspace.bronze.{resource.lower()}"
        )
        
        self.logger.info(f"bronze columns: {bronze_df.columns} for resource: {resource}")

        metadata_columns = [
            col("row_hash"),
            col("api_url"),
            col("load_timestamp")
        ]

        mapped_columns = [
                    coalesce(expr(source_path), lit(None)).alias(target_name)
                    for target_name, source_path
                    in self.resource_mappings[resource]["columns"].items()
                    ]
        self.logger.info(f"mapped columns: {mapped_columns} for resource: {resource}")
        

        silver_df = bronze_df.select(
            *metadata_columns,
            *mapped_columns
        )

        return silver_df
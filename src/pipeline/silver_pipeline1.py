from pyspark.sql.functions import col
from config.settings import settings
import re

class SilverPipeline:

    def __init__(self, spark):
        self.spark = spark
        self.resource_mappings = settings.get()['resource_mappings']

    def _build_column_expression(self, base_path, source_path):
        """
        Build a PySpark column expression that handles array indexing and nested structs.
        
        Examples:
        - "id" -> col("resource_json.id")
        - "class.code" -> col("resource_json.class.code")
        - "address[0].city" -> col("resource_json.address").getItem(0).city
        """
        # Start with the base column
        column_expr = col(base_path)
        
        # Parse the source path for array indexing and nested fields
        # Split by array indexing pattern like [0], [1], etc.
        parts = re.split(r'(\[\d+\])', source_path)
        
        for part in parts:
            if not part:  # Skip empty strings
                continue
            
            # Check if this is an array index like [0]
            array_match = re.match(r'\[(\d+)\]', part)
            if array_match:
                index = int(array_match.group(1))
                column_expr = column_expr.getItem(index)
            else:
                # This is a field path (could be nested like "address" or "class.code")
                # Add it to the column expression
                for field in part.split('.'):
                    if field:  # Skip empty strings from leading/trailing dots
                        column_expr = column_expr.getField(field)
        
        return column_expr


    def run(self, resource):

        bronze_df = self.spark.read.table(f"workspace.bronze.{resource.lower()}" )
        metadata_columns = [
            col("row_hash"),
            col("api_url"),
            col("load_timestamp")]
        
        # Build column expressions that handle array indexing and nested structs
        columns = [
            self._build_column_expression("resource_json", source_path).alias(target_name)
            for target_name, source_path in self.resource_mappings[resource]['columns'].items()
        ]
        
        silver_df = bronze_df.select(*metadata_columns, *columns)
        silver_df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.silver.{resource.lower()}")

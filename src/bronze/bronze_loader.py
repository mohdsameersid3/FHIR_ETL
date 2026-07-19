from pyspark.sql.functions import (
    current_timestamp,
    col,
    sha2,
    to_json,
    explode)

class BronzeLoader:

    def __init__(self, spark):
        self.spark = spark

    def load(self, resource):

        raw_path = f"/Volumes/workspace/default/FHIR/raw/{resource}"

        df = (
            self.spark.read
            .option("recursiveFileLookup", "true")
            .option("multiline", "true")
            .json(raw_path)
        )

        # Check if 'entry' column exists, filter out records without it
        if "entry" not in df.columns:
            raise ValueError(f"No 'entry' column found in JSON data. Available columns: {df.columns}")
        
        # Filter out rows where entry is null before exploding
        df_with_entries = df.filter(col("entry").isNotNull())
        
        # Explode and transform in one go using select with multiple expressions
        bronze_df = df_with_entries.select(
            explode("entry").alias("entry_json")
        ).select(
            current_timestamp().alias("load_timestamp"),
            sha2(to_json(col("entry_json")), 256).alias("row_hash"),
            col("entry_json.fullUrl").alias("api_url"),
            col("entry_json.resource").alias("resource_json")
        ).select(
            "load_timestamp",
            "row_hash",
            "api_url",
            "resource_json.*"
        )

        return bronze_df

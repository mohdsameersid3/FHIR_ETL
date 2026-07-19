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

        exploded_df = df.select( "*", explode("entry").alias("entry_json"))
        bronze_df = exploded_df.withColumn("resource_json", col("entry_json.resource") )

        bronze_df = bronze_df.withColumn(
            "load_timestamp",
            current_timestamp()
        )

        bronze_df = bronze_df.withColumn(
            "api_url",
            # get_json_object("entry_json","$.fullUrl")
             col("entry_json.fullUrl")
        )

        bronze_df = bronze_df.withColumn(
            "row_hash",
            sha2(to_json(col("entry_json")), 256)
        )

        bronze_df = bronze_df.select(
                "load_timestamp",
                "row_hash",
                "api_url",
                "resource_json" )

        return bronze_df
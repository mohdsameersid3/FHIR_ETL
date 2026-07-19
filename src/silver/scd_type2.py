from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp,col, lit
from src.common.logger import LoggerFactory

class SCDHandler:

    def __init__(self, spark):
        self.spark = spark
        self.logger = LoggerFactory.get_logger(__name__)

    def type2handler(self, incoming_df):

        existing_df = self.spark.table(f"workspace.silver.{incoming_df}")
        current_df = existing_df.filter("is_current = true")

        new_records = ( incoming_df.alias("incoming").join(current_df.alias("existing"),"patient_id","leftanti") )
        changed_records = ( incoming_df.alias("incoming")
                                .join(current_df.alias("existing"),"patient_id")
                                .filter(col("incoming.row_hash") != col("existing.row_hash")) )
        changed_count = changed_records.count()
        new_count = new_records.count()

        self.logger.info(f"Changed Patients: {changed_count}")
        self.logger.info(f"New records: {new_count}")

        if changed_count == 0:
            self.logger.info(f"no new or changed patients")
            return new_records

        delta_table = DeltaTable.forName(self.spark, f"workspace.silver.{incoming_df}")

        (
            delta_table.alias("target").merge(changed_records.select("patient_id").alias("source"),
                """
                target.patient_id = source.patient_id
                AND target.is_current = true
                """).whenMatchedUpdate( set={
                                    "effective_to": "current_timestamp()",
                                    "is_current": "false"} ).execute() 
        )

        changed_new_records = (changed_records
                            .withColumn(
                                "effective_from",
                                current_timestamp()
                            )

                            .withColumn(
                                "effective_to",
                                lit(None).cast("timestamp")
                            )

                            .withColumn(
                                "is_current",
                                lit(True)
                            )
                        )
        
        brand_new_rocords = (new_records

                            .withColumn(
                                "effective_from",
                                current_timestamp()
                            )

                            .withColumn(
                                "effective_to",
                                lit(None).cast("timestamp")
                            )

                            .withColumn(
                                "is_current",
                                lit(True)
                            )

                        )
        
        return changed_new_records.unionByName(brand_new_rocords)
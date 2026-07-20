from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, col, lit
from src.common.logger import LoggerFactory


class SCDHandler:
    """
    Handles Slowly Changing Dimension (SCD) Type 2 logic.

    Steps:
        1. Read current active records from Silver table.
        2. Identify new records.
        3. Identify changed records using row_hash.
        4. Expire old records.
        5. Return new versions of changed records + brand new records.
    """

    def __init__(self, spark):
        self.spark = spark
        self.logger = LoggerFactory.get_logger(__name__)

    def type2_handler(self, incoming_df, table_name = 'Patient', business_key = 'Patient_id'):
        """
        Parameters
        ----------
        incoming_df : DataFrame
            Latest incoming records.

        table_name : str
            Silver Delta table name.

        business_key : str
            Business key column.
            Example:
                patient_id
                encounter_id
                observation_id

        Returns
        -------
        DataFrame
            Records to append into the Silver table.
        """

        self.logger.info(f"Starting SCD Type 2 for table: {table_name}")

        # Read existing Silver table
        existing_df = self.spark.table(f"workspace.silver.{table_name}")

        # Keep only active records
        current_df = existing_df.filter(col("is_current") == True)

        # ------------------------------------------------------------------
        # New records
        # ------------------------------------------------------------------

        new_records = (
            incoming_df.alias("incoming")
            .join(
                current_df.alias("existing"),
                business_key,
                "leftanti"
            )
            .cache()
        )

        # ------------------------------------------------------------------
        # Changed records
        # ------------------------------------------------------------------

        changed_records = (
            incoming_df.alias("incoming")
            .join(
                current_df.alias("existing"),
                business_key
            )
            .filter(
                col("incoming.row_hash") != col("existing.row_hash")
            )
            .cache()
        )

        changed_count = changed_records.count()
        new_count = new_records.count()

        self.logger.info(f"Changed Records : {changed_count}")
        self.logger.info(f"New Records     : {new_count}")

        # Nothing to process
        if changed_count == 0 and new_count == 0:
            self.logger.info("No new or changed records found.")
            return None

        # ------------------------------------------------------------------
        # Expire existing records
        # ------------------------------------------------------------------

        if changed_count > 0:

            delta_table = DeltaTable.forName(
                self.spark,
                f"workspace.silver.{table_name}"
            )

            (
                delta_table.alias("target")
                .merge(
                    changed_records
                    .select(business_key)
                    .distinct()
                    .alias("source"),

                    f"""
                    target.{business_key} = source.{business_key}
                    AND target.is_current = true
                    """
                )
                .whenMatchedUpdate(
                    set={
                        "effective_to": "current_timestamp()",
                        "is_current": "false"
                    }
                )
                .execute()
            )

        # ------------------------------------------------------------------
        # New version of changed records
        # ------------------------------------------------------------------

        changed_new_records = (
            changed_records
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

        # ------------------------------------------------------------------
        # Brand new records
        # ------------------------------------------------------------------

        brand_new_records = (
            new_records
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

        # ------------------------------------------------------------------
        # Return records to insert
        # ------------------------------------------------------------------

        if changed_count > 0 and new_count > 0:
            final_df = changed_new_records.unionByName(brand_new_records)

        elif changed_count > 0:
            final_df = changed_new_records

        else:
            final_df = brand_new_records

        self.logger.info("SCD Type 2 completed successfully.")

        return final_df
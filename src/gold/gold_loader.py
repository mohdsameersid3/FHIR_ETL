from pyspark.sql.functions import (
    col,
    count,
    coalesce,
    current_timestamp,
    lit,
    regexp_replace
)
from src.common.logger import LoggerFactory

class GoldPipeline:

    def __init__(self, spark):

        self.spark = spark
        self.logger = LoggerFactory.get_logger(__name__)

    def curate(self):

        # =====================================================
        # Read Silver Tables
        # =====================================================

        patient_df = self.spark.table(
            "workspace.silver.patient"
        )

        encounter_df = self.spark.table(
            "workspace..silver.encounter"
        )

        observation_df = self.spark.table(
            "workspace.silver.observation"
        )

        condition_df = self.spark.table(
            "workspace.silver.condition"
        )

        # =====================================================
        # Build Encounter Metrics
        # =====================================================

        encounter_metrics = (

            encounter_df

            .withColumn(
                "patient_id",
                regexp_replace(
                    col("patient_reference"),
                    "Patient/",
                    ""
                )
            )

            .groupBy("patient_id")

            .agg(
                count("*").alias("encounter_count")
            )

        )

        # =====================================================
        # Build Observation Metrics
        # =====================================================

        observation_metrics = (

            observation_df

            .withColumn(
                "patient_id",
                regexp_replace(
                    col("patient_reference"),
                    "Patient/",
                    ""
                )
            )

            .groupBy("patient_id")

            .agg(
                count("*").alias("observation_count")
            )

        )

        # =====================================================
        # Build Condition Metrics
        # =====================================================

        condition_metrics = (

            condition_df

            .withColumn(
                "patient_id",
                regexp_replace(
                    col("patient_reference"),
                    "Patient/",
                    ""
                )
            )

            .groupBy("patient_id")

            .agg(
                count("*").alias("condition_count")
            )

        )

        # =====================================================
        # Build Gold Patient Summary
        # =====================================================

        gold_df = (

            patient_df.alias("p")

            .join(
                encounter_metrics.alias("e"),
                "patient_id",
                "left"
            )

            .join(
                observation_metrics.alias("o"),
                "patient_id",
                "left"
            )

            .join(
                condition_metrics.alias("c"),
                "patient_id",
                "left"
            )

            .select(

                col("patient_id"),

                col("gender"),

                col("birth_date"),

                col("address"),

                coalesce(
                    col("encounter_count"),
                    lit(0)
                ).alias("encounter_count"),

                coalesce(
                    col("observation_count"),
                    lit(0)
                ).alias("observation_count"),

                coalesce(
                    col("condition_count"),
                    lit(0)
                ).alias("condition_count"),

                current_timestamp().alias("load_timestamp")

            )

        )


        # =====================================================
        # Write Gold Table
        # =====================================================

        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(
                "workspace.gold.gold_patient_summary"
            )
        )

        self.logger.info("Gold Patient Summary Created Successfully")
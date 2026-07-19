from src.silver.silver_loader import SilverLoader
from pyspark.sql.functions import (trim,col,when,to_date,lower,to_timestamp, current_timestamp,lit)
from src.silver.scd_type2 import SCDHandler

class SilverPipeline:

    def __init__(self, spark):

        self.spark = spark
        self.silver_loader = SilverLoader(self.spark)
        self.scd_handler = SCDHandler(self.spark)

    def run_transform(self,resource):

        df = self.silver_loader.load(resource)

        match resource:
            case "Patient":
                patient_df = (
                            df
                            # Trim strings
                            .withColumn("patient_id", trim(col("patient_id")))
                            .withColumn("gender", trim(col("gender")))
                            # .withColumn("city", trim(col("city")))
                            # .withColumn("state", trim(col("state")))
                            # .withColumn("country", trim(col("country")))

                            # Empty string → NULL
                            # .withColumn(
                            #     "city",
                            #     when(col("city") == "", None).otherwise(col("city"))
                            # )

                            # .withColumn(
                            #     "state",
                            #     when(col("state") == "", None).otherwise(col("state"))
                            # )

                            # .withColumn(
                            #     "country",
                            #     when(col("country") == "", None).otherwise(col("country"))
                            # )

                            # Standardize Gender
                            .withColumn(
                                "gender",
                                when(lower(col("gender")) == "male", "Male")
                                .when(lower(col("gender")) == "female", "Female")
                                .otherwise("Unknown")
                            )

                            # Convert Date
                            .withColumn(
                                "birth_date",
                                to_date(col("birth_date"))
                            )

                            # Boolean
                            .withColumn(
                                "active",
                                when(lower(col("active")) == "true", True)
                                .otherwise(False)
                            )
                        # creating these columns for the initial load to handle SDCs
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

                            # Remove invalid business key
                            .filter(col("patient_id").isNotNull())

                            # Remove duplicates
                            .dropDuplicates(["patient_id"])

                        )
                # column_names = [col(column) for column in patient_df.columns]
                # patient_df = self.scd_handler.type2handler(patient_df).select(col(*column_names))
                (patient_df.write
              .format("delta")
              .mode("append")
              .saveAsTable(f"workspace.silver.{resource.lower()}")) 

            case "Observation":
                observation_df = (
                                df

                                .withColumn(
                                    "observation_id",
                                    trim(col("observation_id"))
                                )

                                .withColumn(
                                    "status",
                                    trim(col("status"))
                                )

                                .withColumn(
                                    "observation_code",
                                    trim(col("observation_code"))
                                )

                                .withColumn(
                                    "effective_datetime",
                                    to_timestamp(col("effective_datetime"))
                                )

                                .filter(col("observation_id").isNotNull())

                                .dropDuplicates(["observation_id"])

                            )
                (observation_df.write
              .format("delta")
              .mode("append")
              .saveAsTable(f"workspace.silver.{resource.lower()}")) 

            case "Encounter":
                encounter_df = (
                            df
                            .withColumn("encounter_id", trim(col("encounter_id")))
                            .withColumn("status", trim(col("status")))
                            .withColumn("class", trim(col("class")))

                            .withColumn(
                                "status",
                                when(col("status") == "", None).otherwise(col("status"))
                            )

                            .withColumn(
                                "start_date",
                                to_date(col("start_date"))
                            )

                            .withColumn(
                                "end_date",
                                to_date(col("end_date"))
                            )

                            .filter(col("encounter_id").isNotNull())

                            .dropDuplicates(["encounter_id"])

                        )
                (encounter_df.write
              .format("delta")
              .mode("append")
              .saveAsTable(f"workspace.silver.{resource.lower()}")) 

            case "Condition":
                condition_df = (
                            df
                            .withColumn(
                                "condition_id",
                                trim(col("condition_id"))
                            )

                            .withColumn(
                                "clinical_status",
                                trim(col("clinical_status"))
                            )

                            .withColumn(
                                "verification_status",
                                trim(col("verification_status"))
                            )

                            .withColumn(
                                "condition",
                                trim(col("condition"))
                            )

                            .withColumn(
                                "onset_datetime",
                                to_timestamp(col("onset_datetime"))
                            )

                            .filter(col("condition_id").isNotNull())

                            .dropDuplicates(["condition_id"]))
                
                (condition_df.write
              .format("delta")
              .mode("append")
              .saveAsTable(f"workspace.silver.{resource.lower()}")) 

            case _:
                raise ValueError(f"Unsupported resource: {resource}")

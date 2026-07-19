from src.pipeline.extractor_pipeline import ExtractionPipeline
from src.pipeline.bronze_pipeline import BronzePipeline


class MasterPipeline:

    def __init__(self, dbutils, spark):
        
        self.dbutils = dbutils
        self.extraction = ExtractionPipeline(self.dbutils)
        self.bronze = BronzePipeline(spark)

    def run(self):

        resources = [
            "Patient",
            "Encounter",
            "Observation",
            "Condition"
        ]

        for resource in resources:

            self.extraction.run(resource)
            self.bronze.run(resource)
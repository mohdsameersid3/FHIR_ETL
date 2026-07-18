from src.pipeline.extractor_pipeline import ExtractionPipeline
from src.pipeline.bronze_pipeline import BronzePipeline


class MasterPipeline:

    def __init__(self):

        self.extraction = ExtractionPipeline()
        self.bronze = BronzePipeline()

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
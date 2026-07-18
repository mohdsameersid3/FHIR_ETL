from src.pipeline.extraction_pipeline import ExtractionPipeline


class MasterPipeline:

    def __init__(self):

        self.extraction = ExtractionPipeline()

    def run(self):

        resources = [
            "Patient",
            "Encounter",
            "Observation",
            "Condition"
        ]

        for resource in resources:

            self.extraction.run(resource)
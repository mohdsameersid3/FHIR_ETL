from src.pipeline.extractor_pipeline import ExtractionPipeline
from src.pipeline.bronze_pipeline import BronzePipeline
from src.pipeline.silver_pipeline import SilverPipeline
from src.gold.gold_loader import GoldPipeline
from config.settings import settings

class MasterPipeline:

    def __init__(self, dbutils, spark):
        
        self.dbutils = dbutils
        self.resources = settings.get()['resources']
        self.extraction = ExtractionPipeline(self.dbutils)
        self.bronze = BronzePipeline(spark)
        self.silver = SilverPipeline(spark)
        self.gold = GoldPipeline(spark)


    def run(self):

        for resource in self.resources:

            self.extraction.run(resource)
            self.bronze.run(resource)
            self.silver.run_transform(resource)
        self.gold.curate()
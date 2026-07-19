from src.bronze.bronze_loader import BronzeLoader

class BronzePipeline:

    def __init__(self, spark):

        self.spark = spark
        self.bronze_loader = BronzeLoader(self.spark)

    def run(self,resource):

        df = self.bronze_loader.load(resource)

        (df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(f"workspace.bronze.{resource.lower()}"))  
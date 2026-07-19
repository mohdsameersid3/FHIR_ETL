from src.bronze.bronze_loader import BronzeLoader
from src.common.spark import SparkSessionManager

spark = SparkSessionManager().get_session()

loader = BronzeLoader(spark)


df = loader.load('Patient')

df.display()

print(f"Number of records: {df.count()}")


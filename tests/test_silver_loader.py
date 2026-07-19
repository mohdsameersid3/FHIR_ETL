from src.silver.silver_loader import SilverLoader
# from src.common.file_system import FileSystem
from src.common.spark import SparkSessionManager

# dbutils = FileSystem(dbutils)
spark = SparkSessionManager().get_session()

silver_loader = SilverLoader(spark)
df = silver_loader.load('Encounter')
print(df.columns)

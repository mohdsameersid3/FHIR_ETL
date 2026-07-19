from pyspark.sql import SparkSession

class SparkSessionManager:
    _spark = None

    @classmethod
    def get_session(cls):
        """
        Returns a singleton SparkSession.
        Creates it if it doesn't already exist.
        """
        if cls._spark is None:
            cls._spark = (
                SparkSession.builder
                .appName("FHIR_ETL")
                .getOrCreate()
            )
        return cls._spark
from pathlib import Path

from src.bronze.bronze_loader import BronzeLoader
from src.bronze.metadata_enricher import MetadataEnricher

loader = BronzeLoader()

records = loader.load(
    Path("data/raw/Patient/2026-07-18/page_00001.json")
)

enricher = MetadataEnricher()

rows = enricher.enrich(
    records=records,
    resource="Patient",
    source_file="page_00001.json",
    page_number=1,
    api_params={"_count":100},
    extraction_timestamp="2026-07-18T10:30:00"
)

print(rows[0])
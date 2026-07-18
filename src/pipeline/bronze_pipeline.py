from pathlib import Path

from src.common.file_utils import FileUtils
from src.bronze.bronze_loader import BronzeLoader
from src.bronze.metadata_enricher import MetadataEnricher
from src.bronze.parquet_writer import ParquetWriter


class BronzePipeline:

    def __init__(self):

        self.loader = BronzeLoader()
        self.enricher = MetadataEnricher()
        self.writer = ParquetWriter()

    def run(self,resource):

        raw_folder = Path("data") / "raw" / resource
        files = FileUtils.list_json_files(raw_folder)

        if not files:
            print(f"No raw files found for resource: {resource}")
            return

        for page_number, file in enumerate(files, start=1):

            print(f"\nProcessing {file.name}")
            records = self.loader.load(file)
            extraction_date = file.parent.name
            extraction_timestamp = f"{extraction_date}T00:00:00"

            enriched_records  = self.enricher.enrich(
                records=records,
                resource=resource,
                source_file=file,
                page_number=page_number,
                api_params={},
                extraction_timestamp=extraction_timestamp
            )

            output_path  = self.writer.write(
                records=enriched_records ,
                resource=resource,
                extraction_timestamp=extraction_timestamp,
                page_number=page_number,
                source_file=file.name
            )

            print(f"Written -> {output_path }")
        print(f"\nBronze pipeline completed for {resource}")
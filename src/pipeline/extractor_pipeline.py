from datetime import datetime, UTC

from src.extraction.fhir_client import FHIRClient
from src.extraction.paginator import Paginator
from src.extraction.raw_writer import RawWriter
from src.common.checkpoint import CheckpointManager
from src.common.logger import LoggerFactory

from config.settings import settings


class ExtractionPipeline:

    def __init__(self, dbutils):

        self.dbutils = dbutils
        self.client = FHIRClient()
        self.paginator = Paginator(self.client)
        self.writer = RawWriter(self.dbutils)
        self.checkpoint_manager = CheckpointManager(self.dbutils)
        self.logger = LoggerFactory.get_logger(__name__)

        config = settings.get()
        self.page_size = config['api']['page_size']
        self.initial_load_date = config['load_time']['initial_load_date']

    def run(self, resource, params=None):
        extraction_timestamp = datetime.now(UTC)
        page_number = 1
        total_records = 0
        self.checkpoint_manager.load()

        last_timestamp = self.checkpoint_manager.get_incremental_timestamp(resource)
        self.logger.info(f"Starting extraction for {resource}")

        if last_timestamp is None:
            # Initial full load
            self.logger.info("This is initial load")
            params = {"_count": self.page_size,
                "_lastUpdated": f"ge{self.initial_load_date}"}
            self.logger.info(f"Starting initial full load from {params.get(self.initial_load_date,None)}")
        else:
            # Incremental load
            self.logger.info(f"Incremental load from {last_timestamp}")
            params = {
                "_count": self.page_size,
                "_lastUpdated": f"gt{last_timestamp}"}
            self.logger.info(f"Starting incremental load from {last_timestamp}")

        try:
            for bundle in self.paginator.fetch_pages(resource, params):
                file_path = self.writer.write(resource=resource, page_number=page_number, bundle=bundle)
                print(f"Saved -> {file_path}")
                records = len(bundle.get("entry", []))
                total_records = total_records + len(bundle.get("entry", []) if 'entry' in bundle else [])
                self.checkpoint_manager.save(resource = resource, last_page = page_number, total_records = total_records, status = 'SUCCESS' )
                self.logger.info(
                    f"Page={page_number} | "
                    f"Records={records} | "
                    f"Saved={file_path}")
                page_number += 1
            self.logger.info(
                    f"{resource} extraction completed successfully.")

        except Exception as e:
            self.logger.exception(e)
            self.checkpoint_manager.save(
                resource=resource,
                last_page=page_number - 1,
                total_records=total_records,
                status="FAILED"
            )
            raise


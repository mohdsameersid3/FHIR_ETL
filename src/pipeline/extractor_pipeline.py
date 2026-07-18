from datetime import datetime, UTC

from src.extraction.fhir_client import FHIRClient
from src.extraction.paginator import Paginator
from src.extraction.raw_writer import RawWriter
from src.common.checkpoint import CheckpointManager
from src.common.logger import LoggerFactory

from config.settings import settings


class ExtractionPipeline:

    def __init__(self):

        self.client = FHIRClient()
        self.paginator = Paginator(self.client)
        self.writer = RawWriter()
        self.checkpoint = CheckpointManager()
        self.logger = LoggerFactory.get_logger(self.__class__.__name__)

        config = settings.get()
        self.page_size = config['api']['page_size']
        self.max_pages = config['pipeline']['max_pages']
        self.params = {'_count':self.page_size}

    def run(self, resource, params=None):
        extraction_timestamp = datetime.now(UTC)
        page_number = 1
        total_records = 0
        self.logger.info(f"Starting extraction for {resource}")
        try:
            for bundle in self.paginator.fetch_pages(resource, self.params):
                file_path = self.writer.write(
                    resource=resource,
                    page_number=page_number,
                    bundle=bundle)
                records = len(bundle.get("entry", []))
                total_records += records
                self.logger.info(
                    f"Page={page_number} | "
                    f"Records={records} | "
                    f"Saved={file_path}"
                )
                if self.max_pages is not None and page_number >= self.max_pages:
                    self.logger.info(
                        f"Reached development limit of {self.max_pages} pages."
                    )
                    break
                page_number += 1
            self.checkpoint.save(
                    resource=resource,
                    last_page=page_number,
                    total_records=total_records,
                    status="SUCCESS")
        
            self.logger.info(
                    f"{resource} extraction completed successfully.")

        except Exception as e:
            self.logger.exception(e)
            self.checkpoint.save(
                resource=resource,
                last_page=page_number - 1,
                total_records=total_records,
                status="FAILED"
            )
            raise


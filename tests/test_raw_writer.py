from src.extraction.fhir_client import FHIRClient
from src.extraction.paginator import Paginator
from src.extraction.raw_writer import RawWriter
from src.common.checkpoint import CheckpointManager
from src.common.logger import LoggerFactory
from src.common.file_system import FileSystem

dbutils = FileSystem(dbutils)

client = FHIRClient()
checkpoint_manager = CheckpointManager(dbutils)
logger = LoggerFactory().get_logger(__name__)
logger.info("Checkpoint has began")
paginator = Paginator(client)
writer = RawWriter(dbutils)
logger.info("files are being saved")

page_number = 1
total_records = 0
checkpoint_manager.load()
last_timestamp = checkpoint_manager.get_incremental_timestamp("Patient")

if last_timestamp is None:
    # Initial full load from 2025-01-01
    params = {
        "_count": 100,
        "_lastUpdated": "ge2026-07-01"
    }
    logger.info("Starting initial full load from 2025-01-01")
else:
    # Incremental load
    params = {
        "_count": 100,
        "_lastUpdated": f"gt{last_timestamp}"}
    logger.info(f"Starting incremental load from {last_timestamp}")

for bundle in paginator.fetch_pages(
    resource="Patient",
    params=params ):

    file_path = writer.write(resource="Patient", page_number = page_number, bundle=bundle)

    print(f"Saved -> {file_path}")
    total_records = total_records + len(bundle.get("entry", []) if 'entry' in bundle else [])
    checkpoint_manager.save(resource = "Patient", last_page = page_number, total_records=total_records, status = 'SUCCESS' )

    page_number += 1

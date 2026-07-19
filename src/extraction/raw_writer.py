import json
from datetime import datetime, UTC
from src.common.logger import LoggerFactory
from src.common.checkpoint import CheckpointManager

class RawWriter:

    def __init__(self, dbutils, base_path="/Volumes/workspace/default/FHIR/raw"):

        self.base_path = base_path
        self.dbutils = dbutils
        self.logger = LoggerFactory().get_logger(__name__)

        self.checkpoint_manager = CheckpointManager(self.dbutils)

    def write(self, resource, page_number, bundle):
        extraction_date = datetime.now(UTC).strftime("%Y-%m-%d")
        output_dir = f"{self.base_path}/{resource}/{extraction_date}"
        self.logger.info(f"writing to {output_dir}")
        # Creates directory if it doesn't exist
        self.dbutils.mkdir(output_dir)
        output_file = f"{output_dir}/page_{page_number:05d}.json"
        self.logger.info(f"writing file {output_file}")
        self.dbutils.write_json(output_file, bundle)
        self.logger.info(f"successfully written file {output_file}")
        return output_file
    


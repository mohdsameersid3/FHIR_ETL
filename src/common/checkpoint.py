import json
from datetime import datetime, UTC
from src.common.logger import LoggerFactory

class CheckpointManager:

    def __init__(self, dbutils,
        checkpoint_file="/Volumes/workspace/default/fhir/checkpoint/checkpoint.json"):

        self.checkpoint_file = checkpoint_file
        self.dbutils = dbutils
        # Create checkpoint directory if it doesn't exist
        checkpoint_dir = "/".join(self.checkpoint_file.split("/")[:-1])
        self.dbutils.mkdir(checkpoint_dir)

        self.logger = LoggerFactory.get_logger(__name__)

    def load(self):

        # Create empty checkpoint file if it doesn't exist
        self.logger.info("Checking for checkpoint file")
        try:
            checkpoint_data = self.dbutils.read_json(self.checkpoint_file)
            self.logger.info("Checkpoint file exists")
            return json.loads(checkpoint_data)
        except Exception:
            self.logger.info("Checkpoint file doesn't exist creating a new one")
            self.dbutils.write_json(
                self.checkpoint_file,
                json.dumps({}, indent=4)
                )
            return {}

    def save(self, resource, last_page, total_records, status, incremental_from=None):

        checkpoint = self.load()
        current_time = datetime.now(UTC).isoformat()
        
        checkpoint = { resource:
            {"last_successful_run": current_time,
            "incremental_from": current_time or incremental_time,
            "last_page": last_page,
            "total_records": total_records,
            "status": status
            }
            }
        self.logger.info(f"Saving checkpoint for {checkpoint[resource]} to {self.checkpoint_file}")
        self.dbutils.write_json(self.checkpoint_file, json.dumps(checkpoint, indent=4))
        self.logger.info(f"Checkpoint {resource} saved successfullly to {self.checkpoint_file}")

        self.logger.info( f"for Resource : {resource}\n "
                        f"Last successful run: {checkpoint[resource]['last_successful_run']}\n "
                         f"Last page: {checkpoint[resource]['last_page']}\n "
                         f"Total records fetched: {checkpoint[resource]['total_records']}\n "
                         f"Status: {checkpoint[resource]['status']}\n" )

    def get_incremental_timestamp(self, resource):
        """Returns the incremental timestamp for the given resource"""
        checkpoint = self.load()
        return checkpoint.get(resource, {}).get("incremental_from")










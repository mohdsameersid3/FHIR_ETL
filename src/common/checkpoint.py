import json
from datetime import datetime, UTC

from src.common.logger import LoggerFactory


class CheckpointManager:

    def __init__(
        self,
        dbutils,
        checkpoint_file="/Volumes/workspace/default/FHIR/checkpoint/checkpoint.json"
    ):

        self.dbutils = dbutils
        self.checkpoint_file = checkpoint_file

        checkpoint_dir = "/".join(checkpoint_file.split("/")[:-1])
        self.dbutils.mkdir(checkpoint_dir)

        self.logger = LoggerFactory.get_logger(__name__)

    def load(self):
        """
        Loads the checkpoint file.
        Creates an empty checkpoint if it doesn't exist.
        """

        try:
            checkpoint = self.dbutils.read_json(self.checkpoint_file)
            self.logger.info("Checkpoint loaded successfully.")
            return checkpoint

        except Exception:
            self.logger.info("Checkpoint not found. Creating a new checkpoint.")

            checkpoint = {}

            self.dbutils.write_json(
                self.checkpoint_file,
                checkpoint
            )

            return checkpoint

    def save(
        self,
        resource,
        last_page,
        total_records,
        status,
        incremental_from=None
    ):
        """
        Saves or updates checkpoint information for a resource.
        """

        checkpoint = self.load()

        current_time = datetime.now(UTC).isoformat()

        checkpoint[resource] = {
            "last_successful_run": current_time,
            "incremental_from": incremental_from or current_time,
            "last_page": last_page,
            "total_records": total_records,
            "status": status
        }

        self.dbutils.write_json(
            self.checkpoint_file,
            checkpoint
        )

        self.logger.info(
            f"Checkpoint saved for {resource}\n"
            f"Last Successful Run : {checkpoint[resource]['last_successful_run']}\n"
            f"Incremental From    : {checkpoint[resource]['incremental_from']}\n"
            f"Last Page           : {checkpoint[resource]['last_page']}\n"
            f"Total Records       : {checkpoint[resource]['total_records']}\n"
            f"Status              : {checkpoint[resource]['status']}"
        )

    def get_incremental_timestamp(self, resource):
        """
        Returns the last successful incremental timestamp for a resource.
        """

        checkpoint = self.load()

        return (
            checkpoint
            .get(resource, {})
            .get("incremental_from")
        )
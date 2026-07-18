from pathlib import Path
from datetime import datetime, UTC
import json

class CheckpointManager:

    def __init__(self, checkpoint_file = "data/checkpoint/checkpoint.json"):

        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_file.parent.mkdir(parents=True,exist_ok=True)

        if not self.checkpoint_file .exists():
            self.checkpoint_file.write_text("{}")
        
    def load(self):

        with open(self.checkpoint_file, "r") as file:
            return json.load(file)
    
    def save(self, resource, last_page, total_records, status):

        checkpoint = self.load()

        checkpoint[resource] = {
            "last_successful_run": datetime.now(UTC).isoformat(),
            "last_page": last_page,
            "total_records": total_records,
            "status": status}

        with open(self.checkpoint_file, "w") as file:

            json.dump(
                checkpoint,
                file,
                indent=4
            )





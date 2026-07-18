import json
from pathlib import Path
from datetime import datetime, UTC

class RawWriter:

    def __init__(self, base_path="data/raw"):

        self.base_path = Path(base_path)
    
    def write(self, resource, page_number, bundle):
        extraction_date = datetime.now(UTC).strftime("%Y-%m-%d")
        output_dir = self.base_path / resource / extraction_date
        output_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"page_{page_number:05d}.json"
        output_file = output_dir / file_name
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(bundle, file, indent=2)
        return output_file
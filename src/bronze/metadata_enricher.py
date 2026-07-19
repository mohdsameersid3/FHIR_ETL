from datetime import datetime, UTC
import uuid
from pathlib import Path
import hashlib
import json

class MetadataEnricher:

    def __init__(self):

        self.run_id = str(uuid.uuid4())
        self.load_timestamp = datetime.now(UTC).isoformat()

    def calculate(self, record):

        record_json = json.dumps(
            record,
            sort_keys=True)

        return hashlib.sha256(record_json.encode("utf-8")).hexdigest()

    def enrich(
        self,
        records,
        resource,
        page_number,
        api_params,
        extraction_timestamp
    ):

        enriched = []

        for record in records:

            row = record.copy()

            row["row_hash"] = self.calculate(record)

            row["resource_name"] = resource

            row["page_number"] = page_number

            row["api_url_or_params"] = str(api_params)

            row["extraction_timestamp"] = extraction_timestamp

            row["load_timestamp"] = self.load_timestamp

            row["ingestion_run_id"] = self.run_id
            
            enriched.append(row)

        return enriched
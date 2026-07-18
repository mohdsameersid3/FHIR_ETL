from datetime import datetime, UTC
import uuid
from pathlib import Path


class MetadataEnricher:

    def __init__(self):

        self.run_id = str(uuid.uuid4())
        self.load_timestamp = datetime.now(UTC).isoformat()

    def enrich(
        self,
        records,
        resource,
        source_file,
        page_number,
        api_params,
        extraction_timestamp
    ):

        enriched = []

        for record in records:

            row = record.copy()

            row["resource_name"] = resource

            row["source_file"] = Path(source_file).name

            row["page_number"] = page_number

            row["api_url_or_params"] = str(api_params)

            row["extraction_timestamp"] = extraction_timestamp

            row["load_timestamp"] = self.load_timestamp

            row["ingestion_run_id"] = self.run_id

            enriched.append(row)

        return enriched
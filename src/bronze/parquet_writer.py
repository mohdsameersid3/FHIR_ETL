from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import json
from datetime import datetime, UTC

class ParquetWriter:

    def write(
                self,
                records,
                resource,
                extraction_timestamp,
                page_number,
                source_file):
        if not records:
            return
        table = pa.Table.from_pylist(records)
        date = extraction_timestamp.split("T")[0]
        output_dir = (Path("data") / "bronze" / resource / date)
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"batch_{page_number:05d}.parquet"
        metadata = {
                    "resource": resource,
                    "page_number": page_number,
                    "record_count": len(records),
                    "source_file": f"page_{page_number:05d}.json",
                    "parquet_file": file_path.name,
                    "extraction_timestamp": extraction_timestamp,
                    "created_at": datetime.now(UTC).isoformat()
                }
        metadata_path = (output_dir / f"batch_{page_number:05d}.metadata.json")
        with open(metadata_path, "w") as file:
            json.dump(metadata,file,indent=4)
        pq.write_table(
            table,
            file_path)
        return file_path
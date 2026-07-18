from src.extraction.fhir_client import FHIRClient
from src.extraction.paginator import Paginator
from src.extraction.raw_writer import RawWriter

client = FHIRClient()
paginator = Paginator(client)
writer = RawWriter()

page_number = 1

for bundle in paginator.fetch_pages(
    resource="Patient",
    params={"_count": 5} ):

    file_path = writer.write(resource="Patient",page_number=page_number,bundle=bundle)

    print(f"Saved -> {file_path}")

    page_number += 1

    if page_number > 3:
        break
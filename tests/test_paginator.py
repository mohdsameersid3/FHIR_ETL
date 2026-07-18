from src.extraction.fhir_client import FHIRClient
from src.extraction.paginator import Paginator

client = FHIRClient()
paginator = Paginator(client)
count = 0

for page in paginator.fetch_pages(resource = 'Patient', params = {'_count':5}):
    count += 1
    print(f"Page {count}")

    print(len(page.get("entry", [])))

    if count == 3:
        break

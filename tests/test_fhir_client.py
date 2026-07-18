from src.extraction.fhir_client import FHIRApiClient

client  = FHIRApiClient()

bundle = client.get("Patient", params = {"_count":5})

print(bundle["resourceType"])
print(len(bundle["entry"]))
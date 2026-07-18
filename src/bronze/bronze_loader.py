import json
from pathlib import Path


class BronzeLoader:

    def load(self, file_path):

        with open(file_path, "r", encoding="utf-8") as file:
            bundle = json.load(file)
        records = []
        for entry in bundle.get("entry", []):
            resource = entry.get("resource")
            if resource:
                records.append(resource)
        return records
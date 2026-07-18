import hashlib
import json


class HashCalculator:

    @staticmethod
    def calculate(record):

        record_json = json.dumps(
            record,
            sort_keys=True
        )

        return hashlib.sha256(
            record_json.encode("utf-8")
        ).hexdigest()
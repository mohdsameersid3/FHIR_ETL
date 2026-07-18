from pathlib import Path

from src.bronze.bronze_loader import BronzeLoader

print("Program Started")

loader = BronzeLoader()

file = Path("data/raw/Patient/2026-07-18/page_00001.json")

print(f"File exists: {file.exists()}")
print(f"File: {file}")

records = loader.load(file)

print(f"Number of records: {len(records)}")

if len(records) > 0:
    print(records[0])
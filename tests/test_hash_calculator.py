from src.common.hash_calculator import HashCalculator

record = {

    "id":"123",

    "name":"John"

}

hash1 = HashCalculator.calculate(record)

print(hash1)
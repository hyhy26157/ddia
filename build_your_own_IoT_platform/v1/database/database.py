"""
json file as a database
"""
from dataclasses import dataclass, asdict
from pathlib import Path
import json
DATABASE_PATH = Path('/home/darius/ddia/build_your_own_IoT_platform/v1/database/database.json')

def create_database():
    """
    use json file as a database.
    """
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not DATABASE_PATH.exists():
        DATABASE_PATH.touch() 

def post_database(data:dataclass):
    data_json = asdict(data)
    print("data_json:",data_json)
    with DATABASE_PATH.open("w", encoding="utf-8") as f:
        json.dump(data_json, f, indent=2)

def read_database():
    with DATABASE_PATH.open("r", encoding="utf-8") as f:
        loaded = json.load(f)



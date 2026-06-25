import json
import math
from pymongo import MongoClient
import pandas as pd
from pymongo.synchronous.database import Database

def connect() -> Database:
    try:
        uri = "mongodb://admin:admin@127.0.0.1:27017/"
        client = MongoClient(uri)
    except Exception as e:
        raise e

    db = client["sbp"]

    try:
        db.create_collection('buildings')
    except Exception as e:
        print("Collection already exists")

    try:
        db.create_collection('neighborhoods')
    except Exception as e:
        print("Collection already exists")

    return db

def read_boundaries(file: str) -> dict:
    geo = {}
    gj = json.load(open(file))
    for feature in gj["features"]:
        code = feature["properties"].get("neighborhood_code")
        geo[code] = {
            "geometry": feature["geometry"],
        }

    return geo

def read_neighborhoods(file: str) -> pd.DataFrame:
    df = pd.read_parquet(file)

    return df

def write_neighborhoods(db: Database, neighborhoods: pd.DataFrame, geo: dict):
    docs = neighborhoods.to_dict("records")

    for doc in docs:
        # NaN -> None so Mongo stores BSON null, not a float NaN
        for key, value in doc.items():
            if isinstance(value, float) and math.isnan(value):
                doc[key] = None
        # merge the matching polygon onto the row (empty dict if no geometry)
        doc.update(geo.get(doc["neighborhood_code"]) or {})

    db.neighborhoods.insert_many(docs)
    print(f"inserted {db.neighborhoods.count_documents({})} neighborhoods")

if __name__ == "__main__":
    db = connect()
    geo = read_boundaries('./data/neighborhood_boundaries.geojson')
    neighborhoods = read_neighborhoods('./data/neighborhoods_history.parquet')
    write_neighborhoods(db, neighborhoods, geo)


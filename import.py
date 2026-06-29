import json
import math

import pandas as pd
import pyarrow.parquet as pq
from pymongo import MongoClient
from pymongo.synchronous.database import Database


def connect() -> Database:
    try:
        uri = "mongodb://admin:admin@127.0.0.1:27017/"
        client = MongoClient(uri)
    except Exception as e:
        raise e

    db = client["sbp"]

    for name in ("buildings", "neighborhoods"):
        db.drop_collection(name)
        db.create_collection(name)

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

    df = df.rename(columns={
        "neighborhood_code": "neighborhood_name",
        "codering": "neighborhood_code",
    })

    return df

def read_enrichment(file: str) -> dict:
    df = pd.read_parquet(file, columns=[
        "codering",
        "bouwjaar_afgelopen_tien_jaar",
        "bouwjaar_meer_dan_tien_jaar_geleden",
        "gemiddeld_aardgasverbruik",
    ])
    out = {}
    for row in df.to_dict("records"):
        code = row.pop("codering")
        out[code] = row
    return out

def write_neighborhoods(db: Database, neighborhoods: pd.DataFrame, geo: dict, enrichment: dict):
    docs = neighborhoods.to_dict("records")

    for doc in docs:
        if doc.get("year") == 2024:
            doc.update(enrichment.get(doc["neighborhood_code"]) or {})
        for key, value in doc.items():
            if isinstance(value, float) and math.isnan(value):
                doc[key] = None
        doc.update(geo.get(doc["neighborhood_code"]) or {})

    db.neighborhoods.insert_many(docs)
    print(f"inserted {db.neighborhoods.count_documents({})} neighborhoods")

def import_buildings(db: Database, file: str, batch_size: int = 50_000):
    total = 0
    for batch in pq.ParquetFile(file).iter_batches(batch_size=batch_size):
        docs = batch.to_pylist()
        for doc in docs:
            lon, lat = doc["lon"], doc["lat"]
            if lon is not None and lat is not None:
                doc["location"] = {"type": "Point", "coordinates": [lon, lat]}
        db.buildings.insert_many(docs)
        total += len(docs)
        print(f"inserted {total:,} buildings")

if __name__ == "__main__":
    db = connect()
    geo = read_boundaries('./data/neighborhood_boundaries.geojson')
    neighborhoods = read_neighborhoods('./data/neighborhoods_history.parquet')
    enrichment = read_enrichment('./data/neighborhoods_2024.parquet')
    write_neighborhoods(db, neighborhoods, geo, enrichment)
    import_buildings(db, './data/buildings.parquet')


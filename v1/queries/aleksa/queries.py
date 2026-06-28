from pymongo import MongoClient
from pymongo.synchronous.database import Database

def connect() -> Database:
    try:
        uri = "mongodb://admin:admin@127.0.0.1:27017/"
        client = MongoClient(uri)
    except Exception as e:
        raise e

    db = client["sbp"]

    return db

def query1(db: Database) -> None:
    pipeline = [
        {
            "$match": {
                "year": 2024,
                "neighborhood_code": {"$regex": "^BU"},
                "gemiddelde_woz_waarde_van_woningen": {"$ne": None},
            }
        },
        {
            "$group": {
                "_id": None,
                "p95": {
                    "$percentile": {
                        "input": "$gemiddelde_woz_waarde_van_woningen",
                        "p": [0.95],
                        "method": "approximate",
                    }
                },
                "neighborhoods": {
                    "$push": {
                        "code": "$neighborhood_code",
                        "woz": "$gemiddelde_woz_waarde_van_woningen",
                    }
                },
            }
        },
        {
            "$project": {
                "topCodes": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$neighborhoods",
                                "as": "n",
                                "cond": {"$gte": ["$$n.woz", {"$arrayElemAt": ["$p95", 0]}]},
                            }
                        },
                        "as": "n",
                        "in": "$$n.code",
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "buildings",
                "let": {"codes": "$topCodes"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$in": ["$neighborhood_code", "$$codes"]},
                                    {"$gt": ["$construction_year", 2000]},
                                    {"$gt": ["$floor_area_m2", 150]},
                                ]
                            }
                        }
                    },
                    {"$group": {"_id": "$municipality_code", "count": {"$sum": 1}}},
                ],
                "as": "perMunicipality",
            }
        },
        {"$unwind": "$perMunicipality"},
        {"$replaceRoot": {"newRoot": "$perMunicipality"}},
        {"$sort": {"count": -1}},
    ]

    for row in db.neighborhoods.aggregate(pipeline):
        print(row)

def query3(db: Database) -> None:
    pipeline= [
        {
            "$match": {
                "neighborhood_code": { "$regex": "^BU" },
                "year": 2024,
                "gemiddelde_woz_waarde_van_woningen": { "$ne": None },
            },
        },
        {
            "$addFields": {
                "distance_sum": { "$add": [ "$afstand_tot_grote_supermarkt", "$afstand_tot_huisartsenpraktijk", "$afstand_tot_kinderdagverblijf", "$afstand_tot_school" ]}
            },
        },
        {
            "$bucketAuto": {
                "groupBy": "$distance_sum",
                "buckets": 20,
                "output": {
                    "percentage_new_buildings": { "$avg": "$bouwjaar_afgelopen_tien_jaar" },
                    "avg_value": { "$avg": "$gemiddelde_woz_waarde_van_woningen"},
                    "avg_distance": { "$avg": "$distance_sum"},
                    "count": { "$sum": 1 }
                }
            }
        }
    ]

    for row in db.neighborhoods.aggregate(pipeline):
        print(row)

def query5(db: Database) -> None:
    pipeline = [
        {
            "$match": {
                "year": 2024,
                "neighborhood_code": { "$regex": "^BU" },
                "gemiddelde_woz_waarde_van_woningen": {"$ne": None},
            }
        },
        {
            "$lookup": {
                "from": "buildings",
                "let": {"code": "$neighborhood_code"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$neighborhood_code", "$$code"],
                            },
                        },
                    },
                    {
                        "$count": "building_count",
                    }
                ],
                "as": "building_count"
            },
        },
        {
            "$project": {
                "neighborhood_code": 1,
                "oppervlakte_totaal": 1,
                "municipality_code": { "$concat": ["GM", { "$substrBytes": ["$neighborhood_code", 2, 4] }] },
                "building_count": { "$first": "$building_count.building_count" },   # array -> number
                "density": {
                    "$divide": [
                        { "$ifNull": [{ "$first": "$building_count.building_count" }, 0] },
                        { "$divide": ["$oppervlakte_totaal", 100] }
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "partitionBy": "$municipality_code",
                "sortBy": { "density": -1 },
                "output": {
                    "rank_in_municipality": { "$rank": {} }
                }
            }
        },
        {
            "$limit": 10
        }
    ]

    for row in db.neighborhoods.aggregate(pipeline):
        print(row)

def query5alt(db: Database) -> None:
    pipeline = [
        {
            "$match": {
                "year": 2024,
                "neighborhood_code": {"$regex": "^BU"},
                "oppervlakte_totaal": {"$gt": 0},
            }
        },
        {
            "$group": {
                "_id": None,
                "neighborhoods": {
                    "$push": {"code": "$neighborhood_code", "area": "$oppervlakte_totaal"}
                },
            }
        },
        {
            "$lookup": {
                "from": "buildings",
                "pipeline": [
                    {"$match": {"neighborhood_code": {"$ne": None}}},
                    {"$group": {"_id": "$neighborhood_code", "count": {"$sum": 1}}},
                ],
                "as": "counts",
            }
        },
        {
            "$addFields": {
                "countMap": {
                    "$arrayToObject": {
                        "$map": {
                            "input": "$counts",
                            "as": "c",
                            "in": {"k": "$$c._id", "v": "$$c.count"},
                        }
                    }
                }
            }
        },
        {
            "$project": {
                "rows": {
                    "$map": {
                        "input": "$neighborhoods",
                        "as": "n",
                        "in": {
                            "neighborhood_code": "$$n.code",
                            "municipality_code": {
                                "$concat": ["GM", {"$substrBytes": ["$$n.code", 2, 4]}]
                            },
                            "building_count": {
                                "$ifNull": [{"$getField": {"field": "$$n.code", "input": "$countMap"}}, 0]
                            },
                            "density": {
                                "$divide": [
                                    {"$ifNull": [{"$getField": {"field": "$$n.code", "input": "$countMap"}}, 0]},
                                    {"$divide": ["$$n.area", 100]},
                                ]
                            },
                        },
                    }
                }
            }
        },
        {"$unwind": "$rows"},
        {"$replaceRoot": {"newRoot": "$rows"}},
        {
            "$setWindowFields": {
                "partitionBy": "$municipality_code",
                "sortBy": {"density": -1},
                "output": {"rank_in_municipality": {"$rank": {}}},
            }
        },
        {"$sort": {"density": -1}},
        {"$limit": 10},
    ]

    for row in db.neighborhoods.aggregate(pipeline):
        print(row)


if __name__ == "__main__":
    db = connect()

    # 1. U kojim opštinama je najveća koncentracija velikih, novijih stanova
    # (preko 150 m², izgrađenih posle 2000) u kvartovima visoke procenjene vrednosti?
    # query1(db)

    # 3. Da li kvartovi sa boljom pristupačnošću sadržaja (škole, supermarketi, lekari, vrtići)
    # imaju veću vrednost nekretnina i noviji fond?
    # query3(db)

    # 5. Koji kvartovi imaju najveću stvarnu gustinu izgrađenosti (broj zgrada po km²)
    # i kako se rangiraju unutar svoje opštine?
    query5alt(db)

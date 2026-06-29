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

def query7(db: Database) -> None:
    pipeline = [
        {
            "$match": {
                "year": 2024,
                "neighborhood_code": { "$regex": "^BU" },
                "gemiddelde_woz_waarde_van_woningen": {"$ne": None},
                "20_personen_met_hoogste_inkomen": { "$ne": None},
                "bouwjaar_afgelopen_tien_jaar": { "$ne": None}
            }
        },
        {
            "$group": {
                "_id": None,
                "neighborhoods": {
                    "$push": {
                        "code": "$neighborhood_code",
                        "area": "$oppervlakte_totaal",
                        "avg_value": "$gemiddelde_woz_waarde_van_woningen",
                        "distance_sum": { "$add": [ "$afstand_tot_grote_supermarkt", "$afstand_tot_huisartsenpraktijk", "$afstand_tot_kinderdagverblijf", "$afstand_tot_school" ]},
                        "percentage_top_20_people": "$20_personen_met_hoogste_inkomen",
                        "percentage_new_buildings": "$bouwjaar_afgelopen_tien_jaar"
                    },
                },
            }
        },
        {
            "$lookup": {
                "from": "buildings",
                "pipeline": [
                    { "$match": { "neighborhood_code": { "$ne": None} } },
                    { "$group": { "_id": "$neighborhood_code", "count": { "$sum": 1 }, "avg_floor_area": {"$avg": "$floor_area_m2"} } },
                    { "$project": { "_id": 0, "code": "$_id", "count": 1, "avg_floor_area": 1 } }
                ],
                "as": "counts"
            }
        },
        {
            "$project": {
                "items": {
                    "$concatArrays": [
                        "$neighborhoods",
                        "$counts"
                    ]
                }
            }
        },
        {
            "$project": {
                "items": 1,
                "_id": 0
            }
        },
        {
            "$unwind": "$items",
        },
        {
            "$group": {
                "_id": "$items.code",
                "doc": { "$mergeObjects": "$items" }
            }
        },
        {
            "$replaceRoot": { "newRoot": "$doc" }
        },
        {
            "$match": {
                "avg_value": { "$exists": True }
            }
        },
        {
            "$addFields": {
                "density": {
                    "$divide": [ "$count",  { "$divide": ["$area", 100] } ]
                }
            }
        },
        {
            "$setWindowFields": {
                "output": {
                    "avg_income": { "$avg": "$percentage_top_20_people" },
                    "avg_new_buildings": { "$avg":  "$percentage_new_buildings"}
                }
            }
        },
        {
            "$addFields": {
                "above_avg_income": {
                    "$cond": {
                        "if": { "$gt": [ "$percentage_top_20_people", "$avg_income" ] },
                        "then": 1,
                        "else": 0
                    }
                },
                "above_avg_new_buildings": { 
                    "$cond": {
                        "if": { "$gt": [ "$percentage_new_buildings", "$avg_new_buildings" ] },
                        "then": 1,
                        "else": 0
                    }
                }
            }
        },
        {
            "$group": {
                "_id": { "income": "$above_avg_income", "age": "$above_avg_new_buildings" },
                "neighborhoods": { "$sum": 1},
                "avg_floor_area": {"$avg": "$avg_floor_area"},
                "avg_density": {"$avg": "$density"},
                "avg_value": {"$avg": "$avg_value"},
                "avg_distance": {"$avg": "$distance_sum"},
            }
        }
    ]

    for row in db.neighborhoods.aggregate(pipeline):
        print(row)

def query9(db: Database) -> None:
    pipeline = [
        {
            "$match": {
                "year": 2024,
                "neighborhood_code": {"$regex": "^BU"},
                "woningvoorraad": {"$ne": None},
                "bouwjaar_meer_dan_tien_jaar_geleden": {"$ne": None},
                "gemiddeld_aardgasverbruik": {"$ne": None},
            }
        },
        {
            "$addFields": {
                "priority": {
                    "$multiply": [
                        "$woningvoorraad",
                        {"$divide": ["$bouwjaar_meer_dan_tien_jaar_geleden", 100]},
                        "$gemiddeld_aardgasverbruik",
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "neighborhood_code": 1,
                "neighborhood_name": 1,
                "woningvoorraad": 1,
                "pct_old": "$bouwjaar_meer_dan_tien_jaar_geleden",
                "avg_gas": "$gemiddeld_aardgasverbruik",
                "priority": 1,
            }
        },
        {"$sort": {"priority": -1}},
        {"$limit": 20},
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
    # query5(db)

    # Kako izgledaju kvartovi podeljeni u četiri grupe po starosti fonda i visini prihoda 
    # i koje su im ključne karakteristike (kvadratura, gustina, vrednost, pristupačnost)?
    # query7(db)

    # Koji su kvartovi prioritet za energetsku obnovu na osnovu starosti fonda, potrošnje gasa i broja stanova?
    query9(db)

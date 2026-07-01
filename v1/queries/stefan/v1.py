import json
import time
import math
from pymongo import MongoClient
import pandas as pd
import pyarrow.parquet as pq
from pymongo.synchronous.database import Database

def connect() -> Database:
    try:
        uri = "mongodb://admin:admin@127.0.0.1:27017/"
        client = MongoClient(uri)
    except Exception as e:
        raise e
    db = client["sbp"]

    return db
def query_1(db: Database) -> None:
    pipeline =  [
        {
            "$match" : {"soort_regio" : "Buurt"}
        },
        {
            "$group" : {
                "_id" : "$neighborhood_code",
                "woz2023" : {"$max" : { "$cond" : [ {"$eq": ["$year", 2023]},   "$gemiddelde_woz_waarde_van_woningen",   0 ]}},
                "woz2024" : {"$max" : { "$cond" : [ {"$eq": ["$year", 2024]},   "$gemiddelde_woz_waarde_van_woningen",   0 ]}},
                
            }
        },
        {
            "$match" : {
                "woz2023" : {"$gt" : 0},
                "woz2024" : {"$gt" : 0}
            }
        },
        {
            "$lookup" : {
                "from" : "buildings",
                "localField" : "_id",
                "foreignField" : "neighborhood_code",
                "as" : "zgrade",
            }
        },
        {
            "$addFields" : {
                "prosek_godina" : {"$avg" : "$zgrade.construction_year"},
                "rast" : {
                    "$divide" : [ { "$subtract" : ["$woz2024", "$woz2023"] } , "$woz2023" ]
                }
            }
        },
        {
            "$unset" : "zgrade"
        },
        {
            "$match" : { "prosek_godina" : {"$gt" : 2000} } },
        {
            "$sort" : { "rast" : -1 }
        },
    ]
    for row in db.neighborhoods.aggregate(pipeline):
        print(row["_id"], row["woz2023"], "->", row["woz2024"],
      f"{row['rast']*100:.1f}%", "god:", round(row["prosek_godina"]))
def query_2(db: Database, year: int) -> None:
    pipeline = [
        {
            "$match" : {"soort_regio" : "Buurt", "year" : year },
        },
        {
            "$limit" : 10
        },
        {
            "$lookup" : {
                "from" : "buildings",
                "localField" : "neighborhood_code",
                "foreignField" : "neighborhood_code",
                "as" : "zgrade"
            }
        },
        {
            "$addFields" : {
                "devijacija" : {"$stdDevPop" : "$zgrade.floor_area_m2"}
            }
        },
        {
            "$bucket" : {
                "groupBy" : "$gemiddelde_woz_waarde_van_woningen",
                "boundaries" : [0,344,467,100000],
                "default" : "ostalo",
                "output" : {
                    "br_kvartova" : {"$sum" : 1},
                    "prosek_devijacije" : {"$avg" : "$devijacija"}
                }

            }
        },

    ]
    for row in db.neighborhoods.aggregate(pipeline):
        print(row)
def query_3(db: Database) -> None:
    pipeline = [
        {
            "$match" : {
                "soort_regio" : "Buurt",
                "year" : 2024,
                "afstand_tot_school":             {"$ne": None},
                "afstand_tot_grote_supermarkt":   {"$ne": None},
                "afstand_tot_huisartsenpraktijk": {"$ne": None},
                "afstand_tot_kinderdagverblijf":  {"$ne": None}
            },
        },
        {
            "$addFields" : {
                "ispunjava" : {
                    "$and" : [
                        { "$lte" : ["$afstand_tot_school", 1]},
                        { "$lte" : ["$afstand_tot_grote_supermarkt", 1]},
                        { "$lte" : ["$afstand_tot_huisartsenpraktijk",1]},
                        { "$lte" : ["$afstand_tot_kinderdagverblijf",1]}
                    ]
                }
            }
        },
        {
            "$group" : {
                "_id" : "$gemeentenaam",
                "kvalifikovani" : {
                    "$sum" : {"$cond" : ["$ispunjava", "$woningvoorraad", 0]}
                },
                "total" : {"$sum" : "$woningvoorraad"}
            }
        },
        {
            "$addFields" : {
                "udeo" : {
                    "$divide" : ["$kvalifikovani", "$total"]
                }
            }
        }
    ]
    for row in db.neighborhoods.aggregate(pipeline):
        print(row)
def query_4(db: Database) -> None:
    pipeline = [
        {
            "$match" : {
                "soort_regio" : "Buurt",
                "year" : 2024,
                "mediaan_vermogen_van_particuliere_huish" : {"$ne" : None},
                "gemiddeld_aardgasverbruik" : {"$ne" : None},
            }
        },
        {
            "$limit" : 100
        },
        {
            "$lookup" : {
                "from" : "buildings",
                "localField" : "neighborhood_code",
                "foreignField" : "neighborhood_code",
                "as" : "zgrade"
            } 
        },
        {
            "$addFields" : {
                "avg_starost_kvarta" : {
                    "$avg" : "$zgrade.construction_year"
                }
            }
        },
        {
            "$unset" : "zgrade"
        },
        {
            "$addFields" : {
                "starosna_klasa_kvarta" : {
                    "$switch" : {
                        "branches" : [
                            {"case" : {"$lt" : ["$avg_starost_kvarta", 1960]}, "then" : "stariji"},
                            {"case" : {"$lt" : ["$avg_starost_kvarta", 1980]}, "then" : "srednji"},
                            {"case" : {"$lt" : ["$avg_starost_kvarta", 2000]}, "then" : "mladji"},
                        ],
                        "default" : "nov"
                    }
                },
                "imovinska_klasa_kvarta" : {
                    "$switch" : {
                        "branches" : [
                            {"case" : {"$lt" : ["$mediaan_vermogen_van_particuliere_huish", 130]}, "then" : "niska"},
                            {"case" : {"$lt" : ["$mediaan_vermogen_van_particuliere_huish", 290]}, "then" : "srednja"},
                        ],
                        "default" : "visoka"
                    }
                },
            }
        },
        {
            "$group" : {
                "_id" : {"starost" : "$starosna_klasa_kvarta", "imovina" : "$imovinska_klasa_kvarta"},
                "prosecan_gas" : {"$avg" : "$gemiddeld_aardgasverbruik"},
                "broj" : {"$sum" : 1}
            }
        },
        {
            "$sort" : {"_id.imovina" : 1, "_id.starost" : 1}
        }
        
    ]
    for row in db.neighborhoods.aggregate(pipeline):
        print(row)
def query_5(db: Database) -> None:
    pipeline = [
        {
            "$match" : {"soort_regio" : "Buurt", "year" : 2024, "mediaan_vermogen_van_particuliere_huish" : {"$ne" : None}}
        },
        {
            "$lookup" : {
                "from" : "buildings",
                "localField" : "neighborhood_code",
                "foreignField" : "neighborhood_code",
                "as" : "zgrade"
            }
        },
        {
            "$addFields" : {
                "avg_starost" : {
                    "$avg" : "$zgrade.construction_year"
                }
            }
        },
        {
            "$addFields" : {
                "imovinska_klasa" : {
                    "$switch" : {
                        "branches" : [
                            {"case" : {"$lt" : ["$mediaan_vermogen_van_particuliere_huish", 130]}, "then" : "niska"},
                            {"case" : {"$lt" : ["$mediaan_vermogen_van_particuliere_huish", 290]}, "then" : "srednja"}
                        ],
                        "default" : "visoka"
                    }
               },
                "starosna_klasa" : {
                    "$switch" : {
                        "branches" : [
                            {"case" : {"$lt" : ["$avg_starost", 1960]}, "then" : "stari"},
                            {"case" : {"$lt" : ["$avg_starost", 1980]}, "then" : "srednji"},
                            {"case" : {"$lt" : ["$avg_starost", 2000]}, "then" : "mladji"},
                        ],
                        "default" : "nov"
                    }
                }
            }
        },
        {
            "$group" : {
                "_id" : {"starost" : "$starosna_klasa",  "imovina" : "$imovinska_klasa"},
                "avg_gas" : {"$avg" : "$aardgaswoningen"},
                "avg_struja" : {"$avg" : "$woningen_hoofdz_elektr_verwarmd"},
                "avg_daljinsko" : {"$avg" : "$percentage_woningen_met_stadsverwarming"},
                "avg_solar" : {"$avg" : "$woningen_met_zonnestroom"}
            }
        },
        {
            "$sort" : {"_id.imovina" : 1, "_id.starost" : 1}
        }
        
        
    ]
    for row in db.neighborhoods.aggregate(pipeline):
        print(row)
if __name__ == "__main__":
    db = connect()
    # koji su kvartovi između 2023. i 2024. zabležili najveći rast prosečne vrednosti nekretnina, a istovremeno imaju mlad stambeni fond?

    time1 = time.time()
    query_1(db)
    time2 = time.time()
    total = time2-time1
    print(f'query1 V1 izvrsen za {total} sekundi')

    # Imaju li kvartovi sa višom vrednošću nekretnina ujednačeniji ili raznovrsniji stambeni fond po kvadraturi?

    # query_2(db, 2024)
    # Koliki je udeo stambenih jedinica koje zadovoljavaju kriterijum „15-minutnog grada“ (blizina škole, supermarketa, lekara i vrtića), po opštinama?

    # query_3(db)

    # Da li stariji stambeni fond znači veću potrošnju gasa, nezavisno od prihoda kvarta?
    # query_4(db)

    # Kako se kvartovi razlikuju po načinu grejanja (gasoo, struja, daljinsko, solar) u odnosu na prihod i starost fonda?

    # query_5(db)

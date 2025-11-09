import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)

    data = {}
    with open(f"data/raw_data/{today_date}/communes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[ "code", "nom", "population", ]]
    city_data_df.rename(columns={ "code": "id", "nom": "name", "population": "nb_inhabitants" }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)
    city_data_df["created_date"] = date.today()

    print(city_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
def consolidate_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # NANTES
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data["results"])
    nantes_data = raw_data_df[[
        "number",          # ID (source)
        "name",
        "contract_name",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "bike_stands",
        "last_update",
        "available_bikes",
        "available_bike_stands"
    ]].copy()

    nantes_data["last_update"] = date.today()

    nantes_data.rename(columns={
        "number":        "ID",
        "name":          "NAME",
        "contract_name": "CITY_NAME",
        "address":       "ADDRESS",
        "position.lon":  "LONGITUDE",
        "position.lat":  "LATITUDE",
        "status":        "STATUS",
        "bike_stands":   "CAPACITY",
        "last_update":   "CREATED_DATE"
    }, inplace=True)
    nantes_data["ID"] = nantes_data["ID"].astype(str) # comme les ID sont des varchar on changent le types en amont
    nantes_data["CITY_CODE"] = "70000" #code special pour nantes qui n'est pas un code de paris
    nantes_data["CODE"] = nantes_data["ID"] # je n'ai pas trouvé d'autres idée, ici le code
    nantes_data.drop_duplicates(inplace=True)

    nantes_data_used = nantes_data[[
        "ID", "NAME", "CITY_NAME", "CITY_CODE",
        "ADDRESS", "LONGITUDE", "LATITUDE", "STATUS", "CREATED_DATE", "CAPACITY"
    ]]

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_data_used;")

    # PARIS
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as Paris:
        data = json.load(Paris)

    # Paris est une LISTE à la racine (pas "results")
    raw_data_df = pd.json_normalize(data)
    Paris_data = raw_data_df[[
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "capacity",
        "duedate",
        "numbikesavailable",
        "numdocksavailable"
    ]].copy()

    Paris_data["duedate"] = date.today()

    Paris_data.rename(columns={
        "stationcode":            "ID",
        "name":                   "NAME",
        "nom_arrondissement_communes": "CITY_NAME",
        "code_insee_commune":     "CITY_CODE",
        "coordonnees_geo.lon":    "LONGITUDE",
        "coordonnees_geo.lat":    "LATITUDE",
        "is_installed":           "STATUS",
        "capacity":               "CAPACITY",
        "duedate":                "CREATED_DATE"
    }, inplace=True)

    Paris_data["ID"] = Paris_data["ID"].astype(str)
    Paris_data["ADDRESS"] = None # pas d'adresse pour le json de paris
    Paris_data.drop_duplicates(inplace=True)

    Paris_data_used = Paris_data[[
        "ID", "NAME", "CITY_NAME", "CITY_CODE",
        "ADDRESS", "LONGITUDE", "LATITUDE", "STATUS", "CREATED_DATE", "CAPACITY"
    ]]

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM Paris_data_used;")
    #STATION_STATEMENT:

    # 1️ Crée d'abord un DataFrame vide avec les bonnes colonnes
    Station_data_df = pd.DataFrame(columns=[
        "STATION_ID",
        "BICYCLE_DOCKS_AVAILABLE",
        "BICYCLE_AVAILABLE",
        "LAST_STATEMENT_DATE",
        "CREATED_DATE"
    ])

    # 2️ Ajoute des lignes à partir des autres DF

    paris_tmp = pd.DataFrame({
        "STATION_ID": Paris_data["ID"],
        "BICYCLE_DOCKS_AVAILABLE": Paris_data["numdocksavailable"],
        "BICYCLE_AVAILABLE": Paris_data["numbikesavailable"],
        "LAST_STATEMENT_DATE": Paris_data["CREATED_DATE"],
        "CREATED_DATE": date.today()
    })

    nantes_tmp = pd.DataFrame({
        "STATION_ID": nantes_data["ID"],
        "BICYCLE_DOCKS_AVAILABLE": nantes_data["available_bike_stands"],
        "BICYCLE_AVAILABLE": nantes_data["available_bikes"],
        "LAST_STATEMENT_DATE": nantes_data["CREATED_DATE"],
        "CREATED_DATE": date.today()
    })

    # 3 Fusionne (concatène) les deux
    Station_data_df = pd.concat([Station_data_df, paris_tmp, nantes_tmp], ignore_index=True)
    Station_data_df.drop_duplicates(inplace=True)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM Station_data_df;")


    con.close()
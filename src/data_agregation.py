import duckdb


def create_agregate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def agregate_dim_city():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)


def agregate_dim_station():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITY,
        CITY_CODE AS CITY_ID
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)
def agregate_fact_station_statement():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    # Where n'accepte pas les fonctions MAX, MIN Quand je fais une jointure, donc j'utilise un WITH pour créer l'attribut dont j'ai besoin
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
        WITH latest AS (
        SELECT MAX(CREATED_DATE) AS MAX_CREATED_DATE FROM CONSOLIDATE_STATION
        )
        SELECT 
            DS.ID as STATION_ID,
            DC.ID AS CITY_ID,
            CSS.BICYCLE_DOCKS_AVAILABLE,
            CSS.BICYCLE_AVAILABLE,
            CSS.LAST_STATEMENT_DATE,
            CSS.CREATED_DATE
        FROM CONSOLIDATE_STATION_STATEMENT CSS
        LEFT JOIN  DIM_STATION DS 
            ON CSS.STATION_ID = DS.ID
        LEFT JOIN  DIM_CITY DC
            ON DC.ID = DS.CITY_ID
        WHERE CSS.CREATED_DATE = (SELECT MAX_CREATED_DATE FROM latest);
    """

    con.execute(sql_statement)
import duckdb

path_crudo = "datasets/raw/competencia_02_crudo.csv"
path_clase_ternaria = "datasets/processed/competencia_02_clase_ternaria_rank.parquet"
path_file_processed = "datasets/processed/competencia_02_Miranda_Wind_Tour.parquet"


duckdb.sql(
    f"""
        CREATE OR REPLACE TABLE competencia_02 AS (
            SELECT
                *
            FROM read_parquet('{path_clase_ternaria}')
        );
        """
)

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS
        SELECT
            *,
            LAST_DAY((SUBSTR(foto_mes, 1, 4) || '-' || SUBSTR(foto_mes, 5, 2) || '-01')::DATE) AS foto_mes_date
        FROM competencia_02;

    ALTER TABLE competencia_02 DROP COLUMN foto_mes;

    ALTER TABLE competencia_02 RENAME COLUMN foto_mes_date TO foto_mes;
    """
)

df = duckdb.sql(
    """
    SELECT
        MIN(foto_mes),
        MAX(foto_mes),
        COUNT(*) AS n
    FROM competencia_02;
    """
).to_df()

print(df)

duckdb.close()

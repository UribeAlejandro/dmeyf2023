import duckdb

path_crudo = "datasets/raw/competencia_02_crudo.csv"
path_clase_ternaria = "datasets/processed/competencia_02_clase_ternaria_rank.parquet"
path_file_processed = "datasets/processed/competencia_02_Miranda_Wind_Tour.parquet"


duckdb.sql(
    f"""
    CREATE OR REPLACE TABLE competencia_02 AS(
        SELECT
            *
        FROM read_csv_auto('{path_crudo}')
    );
    """
)

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT
            *,
            RANK() OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes DESC) AS rank_foto_mes,
        FROM competencia_02
    );
    """
)

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT
            *,
            rank_foto_mes*-1 + 1 AS rank_foto_mes_2
        FROM competencia_02
        ORDER BY foto_mes DESC
    );
    """
)

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT
            *,
            CASE
                WHEN rank_foto_mes_2 = 0 THEN 'BAJA+2'
                WHEN rank_foto_mes_2 =-1 THEN 'BAJA+1'
                ELSE 'CONTINUA'
            END AS clase_ternaria
        FROM competencia_02
    );
    """
)


duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT
            *
        FROM competencia_02
        WHERE
            foto_mes <= 202105
    );
    """
)

duckdb.sql(
    f"""
    COPY competencia_02
    TO '{path_clase_ternaria}' (FORMAT PARQUET);
    """
)

duckdb.sql(
    """
    ALTER TABLE competencia_02 DROP COLUMN rank_foto_mes;
    """
)


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

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02_unique_clients_baja_2 AS (
        SELECT
            DISTINCT numero_de_cliente AS numero_de_cliente
        FROM competencia_02
        WHERE
            clase_ternaria = 'BAJA+2'
    );
    """
)

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02_churned_clients_history AS (
        SELECT
            c.*
        FROM competencia_02 AS c
        RIGHT JOIN competencia_02_unique_clients_baja_2 AS u
            ON c.numero_de_cliente = u.numero_de_cliente
        ORDER BY
            c.numero_de_cliente ASC,
            c.foto_mes ASC
    );
    """
)

duckdb.sql(
    f"""
        COPY competencia_02_churned_clients_history
        TO '{path_file_processed}' (FORMAT PARQUET);
        """
)

duckdb.close()

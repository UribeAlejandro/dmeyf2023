import duckdb

path_database = "database/dmeyf.db"
path_file_raw = "datasets/competencia_02_clase_ternaria.csv"
path_file_processed = "datasets/competencia_02.csv"
path_file_foto_reporte = "datasets/processed/competencia_02_foto_reporte.csv"
path_file_clase_ternaria = "datasets/processed/competencia_02_clase_ternaria.csv"

con = duckdb.connect(path_database)

con.sql(
    f"""
        CREATE OR REPLACE TABLE competencia_02 AS (
            SELECT
                *
            FROM read_csv_auto('{path_file_raw}')
        );
        """
)

con.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT
            *,
            -- Visa_cconsumos
            LAG(Visa_cconsumos, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_1,
            LAG(Visa_cconsumos, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_2,
            LAG(Visa_cconsumos, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_3,
            LAG(Visa_cconsumos, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_4,
            -- mrentabilidad_lag_1
            LAG(mrentabilidad, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_1,
            LAG(mrentabilidad, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_2,
            LAG(mrentabilidad, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_3,
            LAG(mrentabilidad, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_4
        FROM competencia_02
        ORDER BY numero_de_cliente, foto_mes
    );
    """
)

con.sql(
    """
        ALTER TABLE competencia_02
        ADD COLUMN clase_ternaria VARCHAR(10);
        """
)

con.sql(
    """
        UPDATE competencia_02
        SET clase_ternaria = targets.clase_ternaria
        FROM targets
        WHERE
            competencia_02.numero_de_cliente = targets.numero_de_cliente
            AND competencia_02.foto_mes = targets.foto_mes;
        """
)


con.sql(
    """
        CREATE OR REPLACE TABLE competencia_02_vw_mes_reporte AS (
            SELECT
                foto_mes
                , COUNT(*) as total
            FROM competencia_02
            GROUP BY foto_mes
        );
        """
)

con.sql(
    """
        CREATE OR REPLACE TABLE competencia_02_vw_clase_ternaria AS
            SELECT
                clase_ternaria
                , COUNT(*) as total
            FROM competencia_02
            GROUP BY clase_ternaria;
        """
)

con.sql(
    f"""
        COPY competencia_02_vw_mes_reporte
        TO '{path_file_foto_reporte}' (FORMAT CSV, HEADER);
        """
)

con.sql(
    f"""
        COPY competencia_02_vw_clase_ternaria
        TO '{path_file_clase_ternaria}' (FORMAT CSV, HEADER);
        """
)

con.sql(
    f"""
        COPY competencia_02
        TO '{path_file_processed}' (FORMAT CSV, HEADER);
        """
)

con.close()

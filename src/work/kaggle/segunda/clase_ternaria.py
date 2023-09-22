import duckdb

path_database = 'database/dmeyf.db'
path_file_raw = 'datasets/raw/competencia_02_crudo.csv'
path_file_processed = 'datasets/processed/competencia_02.csv'
path_file_foto_reporte = 'datasets/processed/competencia_02_foto_reporte.csv'
path_file_clase_ternaria = 'datasets/processed/competencia_02_clase_ternaria.csv'

con = duckdb.connect(path_database)

con.sql(f"""
        CREATE OR REPLACE TABLE competencia_02 AS (
            SELECT
                *
            FROM read_csv_auto('{path_file_raw}')
        );
        """
)

con.sql("""
        CREATE OR REPLACE TABLE targets AS
        WITH periodos AS (
                SELECT
                    DISTINCT foto_mes
                FROM competencia_02
            ), clientes AS (
                SELECT
                    DISTINCT numero_de_cliente
                FROM competencia_02
            ), todo AS (
                SELECT
                    numero_de_cliente,
                    foto_mes
                FROM clientes
                CROSS JOIN periodos
            ), clase_ternaria AS (
                SELECT
                    t.numero_de_cliente
                    , t.foto_mes
                    , IF(c.numero_de_cliente IS NULL, 0, 1) AS mes_0
                    , LEAD(mes_0, 1)
                        OVER (
                            PARTITION BY t.numero_de_cliente
                            ORDER BY foto_mes
                            ) AS mes_1
                    , LEAD(mes_0, 2)
                        OVER (
                            PARTITION BY t.numero_de_cliente
                            ORDER BY foto_mes
                            ) AS mes_2
                    , NULL AS clase_ternaria -- AGREGAR LÓGICA
                FROM todo AS t
                LEFT JOIN competencia_02 AS c
                USING (numero_de_cliente, foto_mes)
            ) SELECT
                foto_mes
                , numero_de_cliente
                , clase_ternaria
            FROM clase_ternaria
            WHERE mes_0 = 1;
            """
)

con.sql("""
        ALTER TABLE competencia_02
        ADD COLUMN clase_ternaria VARCHAR(10);
        """
)

con.sql("""
        UPDATE competencia_02
        SET clase_ternaria = targets.clase_ternaria
        FROM targets
        WHERE
            competencia_02.numero_de_cliente = targets.numero_de_cliente
            AND competencia_02.foto_mes = targets.foto_mes;
        """
)


con.sql("""
        CREATE OR REPLACE TABLE competencia_02_vw_mes_reporte AS (
            SELECT
                foto_mes
                , COUNT(*) as total
            FROM competencia_02
            GROUP BY foto_mes
        );
        """
)

con.sql("""
        CREATE OR REPLACE TABLE competencia_02_vw_clase_ternaria AS
            SELECT
                clase_ternaria
                , COUNT(*) as total
            FROM competencia_02
            GROUP BY clase_ternaria;
        """
)

con.sql(f"""
        COPY competencia_02_vw_mes_reporte
        TO '{path_file_foto_reporte}' (FORMAT CSV, HEADER);
        """
)

con.sql(f"""
        COPY competencia_02_vw_clase_ternaria
        TO '{path_file_clase_ternaria}' (FORMAT CSV, HEADER);
        """
)

con.sql(f"""
        COPY competencia_02
        TO '{path_file_processed}' (FORMAT CSV, HEADER);
        """
)

con.close()

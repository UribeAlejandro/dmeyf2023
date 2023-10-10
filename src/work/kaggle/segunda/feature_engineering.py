import duckdb

path_database = "../buckets/b1/database/dmeyf.db"
path_file_raw = "../buckets/b1/datasets/competencia_02_clase_ternaria.csv"
path_file_processed = "../buckets/b1/datasets/competencia_02.csv"
path_file_foto_reporte = "datasets/processed/competencia_02_foto_reporte.csv"
path_file_clase_ternaria = "datasets/processed/competencia_02_clase_ternaria.csv"

duckdb.sql(
    f"""
        CREATE OR REPLACE TABLE competencia_02 AS (
            SELECT
                *
            FROM read_csv_auto('{path_file_raw}')
        );
        """
)

duckdb.sql(
    """
    CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT
            *,
            -- Visa_cconsumos
            LAG(Visa_cconsumos, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_1,
            LAG(Visa_cconsumos, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_2,
            LAG(Visa_cconsumos, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_3,
            LAG(Visa_cconsumos, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS Visa_cconsumos_lag_4,
            -- mrentabilidad
            LAG(mrentabilidad, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_1,
            LAG(mrentabilidad, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_2,
            LAG(mrentabilidad, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_3,
            LAG(mrentabilidad, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_lag_4,

            -- mpasivos_margen
            LAG(mpasivos_margen, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mpasivos_margen_lag_1,
            LAG(mpasivos_margen, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mpasivos_margen_lag_2,
            LAG(mpasivos_margen, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mpasivos_margen_lag_3,
            LAG(mpasivos_margen, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mpasivos_margen_lag_4,

            -- cpayroll_trx
            LAG(cpayroll_trx, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS cpayroll_trx_lag_1,
            LAG(cpayroll_trx, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS cpayroll_trx_lag_2,
            LAG(cpayroll_trx, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS cpayroll_trx_lag_3,
            LAG(cpayroll_trx, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS cpayroll_trx_lag_4,

            -- ctrx_quarter
            LAG(ctrx_quarter, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS ctrx_quarter_lag_1,
            LAG(ctrx_quarter, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS ctrx_quarter_lag_2,
            LAG(ctrx_quarter, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS ctrx_quarter_lag_3,
            LAG(ctrx_quarter, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS ctrx_quarter_lag_4,

            -- mcuenta_corriente
            LAG(mcuenta_corriente, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mcuenta_corriente_lag_1,
            LAG(mcuenta_corriente, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mcuenta_corriente_lag_2,
            LAG(mcuenta_corriente, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mcuenta_corriente_lag_3,
            LAG(mcuenta_corriente, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mcuenta_corriente_lag_4,

            -- mprestamos_personales
            LAG(mprestamos_personales, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mprestamos_personales_lag_1,
            LAG(mprestamos_personales, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mprestamos_personales_lag_2,
            LAG(mprestamos_personales, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mprestamos_personales_lag_3,
            LAG(mprestamos_personales, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mprestamos_personales_lag_4,

            -- mtarjeta_visa_consumo
            LAG(mtarjeta_visa_consumo, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mtarjeta_visa_consumo_lag_1,
            LAG(mtarjeta_visa_consumo, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mtarjeta_visa_consumo_lag_2,
            LAG(mtarjeta_visa_consumo, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mtarjeta_visa_consumo_lag_3,
            LAG(mtarjeta_visa_consumo, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mtarjeta_visa_consumo_lag_4,

            -- mrentabilidad_annual
            LAG(mrentabilidad_annual, 1) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_annual_lag_1,
            LAG(mrentabilidad_annual, 2) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_annual_lag_2,
            LAG(mrentabilidad_annual, 3) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_annual_lag_3,
            LAG(mrentabilidad_annual, 4) OVER (PARTITION BY numero_de_cliente ORDER BY foto_mes) AS mrentabilidad_annual_lag_4

        FROM competencia_02
        ORDER BY numero_de_cliente, foto_mes
    );
    """
)

duckdb.sql(
    """CREATE OR REPLACE TABLE competencia_02 AS (
        SELECT *
        FROM competencia_02
        WHERE foto_mes IN (202010, 202011, 202012, 202101, 202102, 202103, 202104, 202105, 202107)
    )
    """
)

duckdb.sql(
    f"""
        COPY competencia_02
        TO '{path_file_processed}' (FORMAT CSV, HEADER);
        """
)

duckdb.close()

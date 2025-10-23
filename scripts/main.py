# scripts/main.py
import os
import yaml
import logging
from dotenv import load_dotenv
from ingest import run_ingestion
from convert_xlsx_to_csv import process_excel_file
from validation_cleansing import validate_table_and_report_duplicates, deduplicate_and_insert
from snowflake.connector import connect

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(message)s',
    datefmt='%H:%M:%S'
)

load_dotenv()

def apply_sql_replacements(sql_content, env_vars):
    """Aplica todos los reemplazos de variables de entorno"""
    for key, value in env_vars.items():
        sql_content = sql_content.replace(f"${{{key}}}", value)
    return sql_content

def execute_sql_file(conn, sql_content, warehouse=None):
    with conn.cursor() as cursor:
        if warehouse:
            logging.info(f"Cambiando a warehouse: {warehouse}")
            cursor.execute(f"USE WAREHOUSE {warehouse}")
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        for i, stmt in enumerate(statements, 1):
            try:
                cursor.execute(stmt)
            except Exception as e:
                logging.error(f"Error en sentencia {i}: {e}")
                raise
        logging.info(f"Archivo SQL ejecutado con éxito. {i} sentencias ejecutadas.")
        return True

def main():
    logging.info("Iniciando pipeline...")

    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE_COMPUTE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_BRONZE")
    )

    env_replacements = {
        'WH_COMPUTE': os.getenv('SNOWFLAKE_WAREHOUSE_COMPUTE'),
        'WH_ANALYTICS': os.getenv('SNOWFLAKE_WAREHOUSE_ANALYTICS'),
        'DB': os.getenv('SNOWFLAKE_DATABASE'),
        'SCHEMA_BRONZE': os.getenv('SNOWFLAKE_SCHEMA_BRONZE'),
        'SCHEMA_SILVER': os.getenv('SNOWFLAKE_SCHEMA_SILVER'),
        'SCHEMA_GOLD': os.getenv('SNOWFLAKE_SCHEMA_GOLD'),
    }

    pk_map = {
        'ORDERS': ['ROW_ID'],
        'RETURNS': ['ORDER_ID'],
        'PEOPLE': ['REGION']
    }

    try:
        # Reemplazar variables en los scripts SQL: WH, DB, SCHEMAS y tablas dinámicas
        sql_files = [
            'sql/01_setup_infra.sql',
            'sql/02_create_raw_tables.sql',
            'sql/03_transform_curated.sql',
            'sql/04_build_business_views.sql'
        ]
        sql_scripts=[]
        for sql_file in sql_files:
            if not os.path.exists(sql_file):
                raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_file}")
            with open(sql_file, "r") as f:
                sql_content = f.read()
            sql_content = apply_sql_replacements(sql_content, env_replacements)
            sql_scripts.append(sql_content)

        # Reemplazar tablas dinámicamente
        config_path = os.getenv("CONFIG_PATH")
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        sources = config['ingestion']['sources']
        for source in sources:
            for i, table in enumerate(source['tables'], 1):
                table_name_script = f"${{TABLE_0{i}}}"
                table_name_replacement = f"RAW_{table['name']}"
                sql_scripts[1] = sql_scripts[1].replace(table_name_script, table_name_replacement)
                sql_scripts[2] = sql_scripts[2].replace(table_name_script, table_name_replacement)

        # 1. Crear infraestructura para Arquitectura Medallion
        execute_sql_file(conn=conn, sql_content=sql_scripts[0], warehouse=env_replacements['WH_COMPUTE'])
        # 2. Crear tablas en capa RAW
        execute_sql_file(conn=conn, sql_content=sql_scripts[1], warehouse=env_replacements['WH_COMPUTE'])
        # 3. Cargar datos y limpieza básica en capa RAW
        create_csv = config['ingestion']['settings']['create_csv']

        for source in sources:
            if (source['type'] == 'excel' and create_csv):
                try:
                    process_excel_file(config)
                except Exception as e:
                    raise Exception(f"{source['id']}: Conversión de XLSX a CSVs falló")
            try:
                run_ingestion(conn, config)
            except Exception as e:
                logging.error(f"{source['id']}: Algunas tablas fallaron en ingesta")
                raise

            for table in source['tables']:
                table_name = table['name']
                raw_table_name = f"RAW_{table_name}"
                pk_columns = pk_map.get(table_name,[])

                if not pk_columns:
                    logging.warning(f"No hay PK definida para {table_name}, omitiendo validación")
                    continue
                try:
                    validation_result = validate_table_and_report_duplicates(
                        conn, raw_table_name, pk_columns, 
                        schema=env_replacements['SCHEMA_BRONZE']
                    )
                    if not validation_result:
                        raise Exception(f"{source['id']}: Validación de duplicados falló")
                    if validation_result['has_duplicates']:
                        logging.info(f"{raw_table_name}: Duplicados encontrados, deduplicando...")
                        deduplicate_and_insert(
                            conn, validation_result,
                            target_schema=env_replacements['SCHEMA_BRONZE'],
                            target_table=raw_table_name,
                            order_by_columns=pk_columns,
                            keep='first',
                            truncate_target=True
                        )
                        logging.info(f"{raw_table_name}: Deduplicacion completada.")
                    else:
                        logging.info(f"{raw_table_name}: Sin duplicados.")
                except Exception as e:
                    logging.error(f"Error procesando {raw_table_name}: {e}")
                    raise
        logging.info("Ingesta y limpieza en RAW completada exitosamente.")
        try:
            execute_sql_file(conn, sql_content=sql_scripts[2], warehouse=env_replacements['WH_ANALYTICS'])
        except Exception as e:
            logging.error(f"Error en la transformación SILVER: {e}")
            raise
        try:
            execute_sql_file(conn, sql_content=sql_scripts[3], warehouse=env_replacements['WH_ANALYTICS'])
        except Exception as e:
            logging.error(f"Error en la transformación GOLD: {e}")
            raise
        logging.info("Pipeline completado exitosamente. Tablas cargadas y transformadas en Snowflake.")
    except Exception as e:
        logging.error(f"Error en pipeline: {e}")
        try:
            conn.rollback()
        except:
            pass
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
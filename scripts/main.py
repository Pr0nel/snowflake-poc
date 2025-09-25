# scripts/main.py
import os
from snowflake.connector import connect
from dotenv import load_dotenv
import subprocess
import sys
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Cargar variables de entorno desde el archivo .env
load_dotenv()

def execute_sql_file(conn, filepath, warehouse=None):
    try:
        cursor = conn.cursor()
        if warehouse:
            logging.info(f"Cambiando a warehouse: {warehouse}")
            cursor.execute(f"USE WAREHOUSE {warehouse}")
        if not os.path.exists(filepath):
            logging.error(f"Archivo no encontrado: {filepath}")
            return False
        with open(filepath, 'r') as f:
            sql_content = f.read().strip()
        logging.info(f"Ejecutando {os.path.basename(filepath)}")
        if not sql_content:
            logging.warning(f"Archivo vacío: {filepath}")
            return True
        logging.info(f"Ejecutando {os.path.basename(filepath)}")
        cursor.execute(sql_content)
        logging.info(f"{os.path.basename(filepath)}: ejecutado con éxito.")
        return True
    except Exception as e:
        logging.error(f"Error en {os.path.basename(filepath)}: {e}")
    finally:
        cursor.close()

def main():
    logging.info("Iniciando pipeline...")

    conn = connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        session_parameters={'MULTI_STATEMENT_COUNT': 0}
    )
    cursor = conn.cursor()
    try:
        # 1. Crear infraestructura y tablas RAW
        sql_files0102 = [
            'sql/01_setup_infra.sql',
            'sql/02_create_raw_tables.sql'
        ]
        for filepath in sql_files0102:
            if not os.path.exists(filepath):
                logging.error(f"Archivo no encontrado: {filepath}")
                continue
            execute_sql_file(conn=conn, filepath=filepath, warehouse='COMPUTE_WH')
        # 2. Cargar datos a RAW
        try:
            cursor=conn.cursor()
            cursor.execute("USE WAREHOUSE ANALYTICS_WH")
            cursor.execute("USE DATABASE RAPPI_POC")
            cursor.execute("USE SCHEMA RAW")
            subprocess.run([sys.executable, 'scripts/ingest.py'], check=True)
        except Exception as e:
            logging.error(f"Error al cargar datos: {e}")
        finally:
            cursor.close()
        # 3. Transformar a CURATED y BUSINESS
        sql_files0304 = [
            'sql/03_transform_curated.sql',
            'sql/04_build_business_views.sql'
        ]
        for filepath in sql_files0304:
            if not os.path.exists(filepath):
                logging.error(f"Archivo no encontrado: {filepath}")
                continue
            execute_sql_file(conn=conn, filepath=filepath, warehouse='ANALYTICS_WH')

        logging.info("Pipeline completado exitosamente. Tablas cargadas y transformadas en Snowflake.")
    except Exception as e:
        logging.error(f"Error en pipeline: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
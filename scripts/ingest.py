# scripts/ingest.py
import os
import re
import logging
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(message)s',
    datefmt='%H:%M:%S'
)

def validate_source_data(source, temp_dir):
    """
    Valida que los archivos fuente existan y tengan datos.
    Args:
        source (dict): Diccionario con la configuración de la fuente.
        temp_dir (str): Directorio temporal donde se encuentran los CSV.
    Returns:
        list: Lista de errores encontrados (vacía si no hay errores).
    """
    errors = []
    if not os.path.exists(source['path']):
        errors.append(f"Archivo no encontrado: {source['path']}")
        return errors
    for table in source['tables']:
        csv_path = os.path.join(temp_dir, f"{table['name']}.csv")
        if not os.path.exists(csv_path):
            errors.append(f"CSV no encontrado: {csv_path}")
        else:
            df = pd.read_csv(csv_path)
            if df.empty:
                errors.append(f"CSV vacío: {csv_path}")
    return errors

def validate_table_exists(conn, table_name):
    """
    Verifica que la tabla exista antes de cargar.
    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Conexión a Snowflake.
        table_name (str): Nombre de la tabla en Snowflake.
    Returns:
        bool: True si la tabla existe, False en caso contrario.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        exists = len(cursor.fetchall()) > 0
        cursor.close()
        return exists
    except:
        return False

def to_snake_case(name):
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    return name.upper().strip('_')

def load_data_with_write_pandas(conn, df, table_name):
    """
    Carga un DataFrame de Pandas en una tabla de Snowflake usando write_pandas.
    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Conexión a Snowflake.
        df (pd.DataFrame): DataFrame de Pandas con los datos a cargar.
        table_name (str): Nombre de la tabla en Snowflake.
    Returns:
        bool: True: la carga fue exitosa, False: lo contrario.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            database, schema = cursor.fetchone()
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            overwrite=False
        )
        logging.info("Datos cargados exitosamente:")
        logging.info(f"  write_pandas: {database}.{schema}.{table_name}: {nrows} filas en {nchunks} fragmentos.")        
        return success
    except Exception as e:
        logging.error(f"Error al cargar datos con write_pandas: {e}")
        return False

def load_data_with_put_copy_into(conn, csv_file, table_name):
    """
    Carga un archivo CSV en una tabla de Snowflake usando PUT + COPY INTO.
    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Conexión a Snowflake.
        csv_file (str): Ruta al archivo CSV local.
        table_name (str): Nombre de la tabla en Snowflake.
    Returns:
        bool: True: la carga fue exitosa, False: lo contrario.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            database, schema = cursor.fetchone()
            cursor.execute(f"PUT file://{csv_file} @%{table_name} AUTO_COMPRESS=TRUE")
            logging.info(f"  PUT: Archivo {csv_file} subido al stage interno @%{table_name}.")
            cursor.execute(f"""
                COPY INTO {table_name}
                FROM @%{table_name}
                FILE_FORMAT = (TYPE='CSV' FIELD_DELIMITER = ',' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            """)
            logging.info("Datos cargados exitosamente:")
            logging.info(f"  COPY INTO: Carga de datos a {database}.{schema}.{table_name} desde el stage interno @%{table_name}.")
            cursor.execute(f"REMOVE @%{table_name}")
            logging.info(f"Se limpia la tabla @%{table_name} del Stage interno.")
            return True
    except Exception as e:
        logging.error(f"Error al cargar datos con PUT + COPY INTO: {e}")
        return False

def load_data_to_snowflake(conn, table_name, method, csv_path=None, df=None):
    """
    Función wrapper para cargar datos a Snowflake según el método especificado.
    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Conexión a Snowflake.
        table_name (str): Nombre de la tabla en Snowflake.
        method (str): Método de carga ('put_copy_into' o 'write_pandas').
        csv_path (str, optional): Ruta al archivo CSV local (requerido para 'put_copy_into').
        df (pd.DataFrame, optional): DataFrame de Pandas (requerido para 'write_pandas').
    Returns:
        bool: True si la carga fue exitosa, False en caso contrario.
    """
    if method == 'put_copy_into':
        if not csv_path:
            raise ValueError("csv_path requerido para put_copy_into")
        success = load_data_with_put_copy_into(conn, csv_path, table_name)
        return success
    elif method == 'write_pandas':
        if df is None:
            raise ValueError("df requerido para write_pandas")
        success = load_data_with_write_pandas(conn, df, table_name)
        return success
    else:
        raise ValueError(f"Método desconocido '{method}' para la tabla '{table_name}'")

def run_ingestion(conn, config):
    """
    Función principal que recibe conexión existente.
    Args:
        conn (snowflake.connector.connection.SnowflakeConnection): Conexión a Snowflake.
        config (dict): Configuración cargada desde el archivo YAML.
    Returns:
        bool: True si todas las cargas fueron exitosas, False en caso contrario.
    """
    temp_dir = config['ingestion']['settings']['temp_dir']
    results = []
    for source in config['ingestion']['sources']:
        errors = validate_source_data(source, temp_dir)
        if errors:
            for error in errors:
                logging.error(f"Validación fallida: {error}")
            results.append(False)
            continue
        logging.info(f"Cargando datos de: {source['id']}")
        for table in source['tables']:
            table_name = table['name']
            raw_table_name = f"RAW_{table_name}"
            method = table['method']
            csv_path = os.path.join(temp_dir, f"{table_name}.csv")
            if not validate_table_exists(conn, raw_table_name):
                logging.error(f"Tabla '{table_name}' no existe en Snowflake. Saltando carga.")
                results.append(False)
                continue
            df = pd.read_csv(csv_path)
            df.columns = [to_snake_case(col) for col in df.columns]
            df.to_csv(csv_path, index=False)
            try:
                success = load_data_to_snowflake(
                    conn=conn,
                    table_name=raw_table_name,
                    method=method,
                    csv_path=csv_path,
                    df=df
                )
                results.append(success)
            except Exception as e:
                logging.error(f"Error al cargar datos en la tabla '{table_name}': {e}")
                results.append(False)
    success_count = sum(results)
    total_count = len(results)
    logging.info(f"Resumen: {success_count}/{total_count} tablas cargadas exitosamente")
    return all(results)
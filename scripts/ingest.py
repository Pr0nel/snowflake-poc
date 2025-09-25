# scripts/ingest.py
import pandas as pd
import re
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv
import logging

load_dotenv()

def to_snake_case(name):
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    return name.lower().strip('_')

# Leer datos
bank_df = pd.read_json('data/BankChurners.json')
coffee_df = pd.read_excel('data/Coffee_Sales_Raw_Data.xlsx')

# Normalizar nombres de columnas a snake_case
bank_df.columns = [to_snake_case(col) for col in bank_df.columns]
coffee_df.columns = [to_snake_case(col) for col in coffee_df.columns]
# Conversión inteligente de order_date
col = coffee_df["order_date"]

if pd.api.types.is_numeric_dtype(col):
    # Es un número: probablemente número de serie de Excel
    coffee_df["order_date"] = pd.to_datetime(col, unit='D', origin='1899-12-30', errors='coerce')
elif pd.api.types.is_string_dtype(col) or col.dtype == 'object':
    # Es texto: convertir normalmente
    coffee_df["order_date"] = pd.to_datetime(col, errors='coerce')
else:
    # Ya es datetime: no hacer nada
    pass
# Eliminar filas con fecha inválida
coffee_df = coffee_df.dropna(subset=["order_date"])
# Convertir a string ISO para garantizar compatibilidad con Snowflake DATE
coffee_df["order_date"] = coffee_df["order_date"].dt.strftime('%Y-%m-%d')

# Conexión nativa a Snowflake
conn = connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    role=os.getenv('SNOWFLAKE_ROLE'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)

try:
    success1, _, _, _ = write_pandas(
        conn=conn,
        df=bank_df,
        table_name='BANK_CHURNERS_RAW',
        database='RAPPI_POC',
        schema='RAW',
        overwrite=False
    )

    success2, _, _, _ = write_pandas(
        conn=conn,
        df=coffee_df,
        table_name='COFFEE_SALES_RAW',
        database='RAPPI_POC',
        schema='RAW',
        overwrite=False
    )
    
    if success1 and success2:
        logging.info("Se han cargado correctamente los datos en Snowflake")
    else:
        logging.warning("Algunas tablas no se cargaron")

except Exception as e:
    logging.error(f"Error al cargar los datos en Snowflake: {e}")
    raise
finally:
    conn.close()
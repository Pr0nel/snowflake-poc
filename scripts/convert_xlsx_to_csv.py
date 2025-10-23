# scripts/convert_xlsx_to_csv.py
import os
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(message)s',
    datefmt='%H:%M:%S'
)

def process_excel_file(config):
    """Convierte las hojas especificadas de un archivo Excel a CSV.
    Args:
        config: Configuración cargada desde config.yaml (config['ingestion']['sources']). Debe incluir: 
            - ingestion: Configuración de ingestión
                - sources: Lista de fuentes de datos
                    - id: Identificador único de la fuente
                    - path: Ruta al archivo Excel
                    - tables: Lista de tablas con nombres y hojas en el formato:
                        [{'name': 'table_name', 'sheet': 'Sheet1'}, {'name': 'table2', 'sheet': 'Sheet2'}, ...]
            - settings: Configuración general
                - temp_dir: Directorio temporal
    """
    temp_dir = config['ingestion']['settings']['temp_dir']
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    for source in config['ingestion']['sources']:
        logging.info(f"Procesando fuente: {source['id']}")
        excel_file = pd.ExcelFile(source['path'])
        for table in source['tables']:
            sheet_name = table['sheet']
            csv_filename = f"{table['name']}.csv"
            csv_path = os.path.join(temp_dir, csv_filename)
            df = excel_file.parse(sheet_name)
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ["date", "fecha", "fec_"]):
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True, infer_datetime_format=True).dt.strftime("%Y-%m-%d")
                    print(col,len(df[col].unique()))
            df.to_csv(csv_path, index=False)
            logging.info(f"  Sheet '{sheet_name}' guardado en {csv_path}")
# scripts/validation_cleansing.py
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(levelname)s] - %(message)s',
    datefmt='%H:%M:%S'
)

def validate_table_and_report_duplicates(conn, table_name, pk_columns, schema=None):
    """
    Valida tabla y reporta duplicados sin eliminar 
    Args:
        conn: Conexión a Snowflake
        table_name: Nombre de la tabla (UPPERCASE)
        pk_columns: Lista de columnas PK ['COLUMN1', 'COLUMN2']
        schema: Esquema de la tabla
    Returns:
        dict con información de validación y duplicados
    """
    cursor = conn.cursor()
    try:
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
        total_rows = cursor.fetchone()[0]
        if total_rows == 0:
            logging.warning(f"Tabla {full_table_name} está vacía")
            return {
                'has_duplicates': False,
                'duplicate_count': 0,
                'total_rows': 0,
                'valid': False,
                'pk_columns': pk_columns,
                'full_table_name': full_table_name
            }
        
        logging.info(f"Tabla {full_table_name}: {total_rows:,} filas")
        cursor.execute(f"DESCRIBE TABLE {full_table_name}")
        table_columns = {row[0].upper() for row in cursor.fetchall()}
        pk_upper = [col.upper() for col in pk_columns]
        invalid_cols = set(pk_upper) - table_columns
        if invalid_cols:
            logging.error(f"Columnas PK inválidas: {invalid_cols}")
            return {
                'has_duplicates': False,
                'duplicate_count': 0,
                'total_rows': total_rows,
                'valid': False,
                'pk_columns': pk_upper,
                'full_table_name': full_table_name
            }
        
        # Detectar duplicados
        pk_list = ', '.join(pk_upper)
        cursor.execute(f"""SELECT COUNT(*)
                           FROM (SELECT {pk_list}
                                 FROM {full_table_name}
                                 GROUP BY {pk_list}
                                 HAVING COUNT(*) > 1
                            )
        """)
        duplicate_groups = cursor.fetchone()[0]
        if duplicate_groups > 0:
            cursor.execute(f"""SELECT {pk_list}, COUNT(*) as cnt
                               FROM {full_table_name}
                               GROUP BY {pk_list}
                               HAVING COUNT(*) > 1
                               ORDER BY cnt DESC
                               LIMIT 3
            """)
            examples = cursor.fetchall()
            logging.warning(f"Duplicados encontrados en {full_table_name}:")
            logging.warning(f"  Grupos duplicados: {duplicate_groups:,}")
            for ex in examples:
                logging.warning(f"  Ejemplo: {dict(zip(pk_upper, ex[:-1]))} - {ex[-1]} ocurrencias")
            return {
                'has_duplicates': True,
                'duplicate_count': duplicate_groups,
                'total_rows': total_rows,
                'valid': True,
                'pk_columns': pk_upper,
                'full_table_name': full_table_name,
                'table_columns': list(table_columns)
            }
        
        logging.info(f"Sin duplicados en {full_table_name}")
        return {
            'has_duplicates': False,
            'duplicate_count': 0,
            'total_rows': total_rows,
            'valid': True,
            'pk_columns': pk_upper,
            'full_table_name': full_table_name,
            'table_columns': list(table_columns)
        }
        
    except Exception as e:
        logging.error(f"Error en validación: {e}")
        raise
    finally:
        cursor.close()

def deduplicate_and_insert(conn, validation_result, target_schema, target_table, 
                           order_by_columns, keep='first', truncate_target=True):
    """
    Elimina duplicados y los inserta en tabla destino
    Args:
        conn: Conexión a Snowflake
        validation_result: Dict retornado de la funcion validate_table_and_report_duplicates()
        target_schema: Schema destino
        target_table: Tabla destino
        order_by_columns: STRING o lista de columna(s) para ordenar y decidir qué mantener. OVER (PARTITION BY {', '.join(pk_columns)} ORDER BY {order_by_columns} {order_by_clause})
        keep: Registro a mantener entre duplicados. 'first': el primero; 'last': el último
        truncate_target: True, vacía la tabla destino antes de insertar. False, inserta al final de la tabla.
    Returns:
        int: Número de filas insertadas
    """
    if not validation_result['valid']:
        raise ValueError("La validación indicó que la tabla fuente no es válida")
    
    cursor = conn.cursor()
    try:
        source_table = validation_result['full_table_name']
        full_target_table = f"{target_schema}.{target_table}"
        pk_columns = validation_result['pk_columns']
        source_columns = set(validation_result['table_columns'])
        aux_table = f"{full_target_table}_AUX"
        logging.info(f"DEDUPLICACIÓN DE {full_target_table}")
        cursor.execute(f"DESCRIBE TABLE {full_target_table}")
        target_columns = {row[0].upper() for row in cursor.fetchall()}
        logging.info(f"Tabla destino {full_target_table} existe")
        # Obtener columnas comunes entre source y target
        common_columns = sorted(source_columns & target_columns)
        logging.info(f"Columnas a transferir: {len(common_columns)}")
        columns_list = ', '.join(common_columns)
        # Validar order_by_columns
        if isinstance(order_by_columns, list):
            order_by_upper = [col.upper() for col in order_by_columns]
            for col in order_by_upper:
                if col not in source_columns:
                    raise ValueError(f"Columna '{col}' no existe en tabla fuente")
            order_by_clause = ', '.join(order_by_upper)
        else:
            order_by_upper = order_by_columns.upper()
            if order_by_upper not in source_columns:
                raise ValueError(f"Columna '{order_by_upper}' no existe en tabla fuente")
            order_by_clause = order_by_upper
        order_clause = "ASC" if keep == 'first' else "DESC"
        pk_list = ', '.join(pk_columns)
        # Crear tabla auxiliar con deduplicados
        cursor.execute(f"DROP TABLE IF EXISTS {aux_table}")
        if validation_result['has_duplicates']:
            dedup_query = f"""CREATE TABLE {aux_table} AS
                               SELECT {columns_list}
                               FROM (
                                   SELECT *, ROW_NUMBER() OVER (
                                       PARTITION BY {pk_list}
                                       ORDER BY {order_by_clause} {order_clause}
                                   ) AS rn
                                   FROM {source_table}
                               )
                               WHERE rn = 1
            """
            cursor.execute(dedup_query)
        else:
            cursor.execute(f"""CREATE TABLE {aux_table} AS
                               SELECT {columns_list}
                               FROM {source_table}
            """)
        # Contar filas en tabla auxiliar
        cursor.execute(f"SELECT COUNT(*) FROM {aux_table}")
        aux_rows = cursor.fetchone()[0]
        logging.info(f"  Tabla auxiliar creada con {aux_rows:,} filas")
        # Ejecutar con transacción
        conn.autocommit(False)
        try:
            cursor.execute(f"TRUNCATE TABLE {full_target_table}")
            logging.info(f"  {full_target_table} truncada")
            cursor.execute(f"""INSERT INTO {full_target_table} ({columns_list})
                               SELECT {columns_list}
                               FROM {aux_table}
            """)
            # Contar filas antes
            cursor.execute(f"SELECT COUNT(*) FROM {full_target_table}")
            final_rows = cursor.fetchone()[0]
            conn.commit()
            logging.info(f"Insertadas {final_rows:,} filas en {full_target_table}")
            if validation_result['has_duplicates']:
                removed = validation_result['total_rows'] - final_rows
                logging.info(f"  Duplicados removidos: {removed:,}, Filas finales insertadas: {final_rows:,}")
            return final_rows
            
        except Exception as e:
            conn.rollback()
            logging.error(f"  Error en INSERT, rollback ejecutado: {e}")
            raise
        finally:
            conn.autocommit(True)
            cursor.execute(f"DROP TABLE IF EXISTS {aux_table}")
            
    except Exception as e:
        logging.error(f"Error en deduplicación e inserción: {e}")
        raise
    finally:
        cursor.close()
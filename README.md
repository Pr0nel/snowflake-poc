# Snowflake Data Pipeline - Proof of Concept and Analytics

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Snowflake](https://img.shields.io/badge/snowflake-standard-29b5e8.svg)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Descripción

Este repositorio contiene una **prueba de concepto (POC)** desarrollada como parte de una implementación de la **arquitectura Medallion** (Bronze -> Silver -> Gold) en **Snowflake**, diseñada para ingestar, transformar y exponer datos listos para análisis. El objetivo es demostrar habilidades prácticas en:

- Ingesta e integración de datos
- Modelamiento y arquitectura de datos
- Programación en Python y SQL
- Uso de Snowflake
- Calidad, trazabilidad y buenas prácticas de ingeniería de datos

---

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Tecnologías](#tecnologías)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Ejecución](#ejecución)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Vistas de Análisis](#vistas-de-análisis)
- [Validación de Datos](#validación-de-datos)
- [Próximos Pasos](#próximos-pasos)

---

## Arquitectura

```
RAW (Bronze Layer)
├── RAW_ORDERS (10,000 registros)
├── RAW_RETURNS (284 registros)
└── RAW_PEOPLE (3 gerentes regionales)
        ↓
CURATED (Silver Layer)
├── CURATED_ORDERS (enriquecida con métricas)
└── CURATED_PEOPLE (gerentes regionales)
        ↓
BUSINESS (Gold Layer)
├── 8 Vistas de análisis de negocio
├── Análisis de rentabilidad
├── Desempeño regional
└── Métricas KPI
```

### Flujo ETL

1. **Ingesta**: Excel -> CSV -> Snowflake (RAW layer)
2. **Validación**: Detección de duplicados, verificación de integridad
3. **Deduplicación**: Tabla auxiliar + transacciones explícitas
4. **Transformación**: Enriquecimiento con cálculos de negocio (Silver layer)
5. **Análisis**: Vistas materializadas para BI (Gold layer)

---

## Tecnologías

| Capa | Tecnología | Versión |
|------|-----------|---------|
| **Orquestación** | Python | 3.8+ |
| **ETL** | Pandas, Snowflake Connector | 3.17.4+ |
| **Almacenamiento** | Snowflake (Standard Edition) | - |
| **SQL** | Snowflake SQL | - |
| **Infraestructura** | GCP (Snowflake) | - |
| **Seguridad** | PAT (Programmatic Access Token), variables de entorno | - |
| **CI/CD** | (Preparado para Airflow/dbt) | - |

---

## Instalación

### Requisitos Previos

- Python 3.8+
- Cuenta Snowflake (Free Trial compatible)

### 1. Clonar el Repositorio

```
git clone https://github.com/tu-usuario/snowflake-poc.git
cd snowflake-poc
```

### 2. Crear Entorno Virtual

```
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# o
.\venv\Scripts\Activate   # Windows
```

### 3. Instalar Dependencias

```
pip install -r requirements.txt
```

**Dependencias principales:**
- `snowflake-connector-python==3.17.4`
- `pandas>=1.5.0`
- `pyyaml>=6.0`
- `python-dotenv>=0.20.0`

---

## Configuración

### 1. Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```env
# Snowflake Connection
SNOWFLAKE_USER=tu_usuario
SNOWFLAKE_PASSWORD=tu_contraseña
SNOWFLAKE_ACCOUNT=tu_account_id
SNOWFLAKE_ROLE=tu_rol
SNOWFLAKE_WAREHOUSE_COMPUTE=COMPUTE_WH
SNOWFLAKE_WAREHOUSE_ANALYTICS=COMPUTE_WH
SNOWFLAKE_DATABASE=BPOLABS_POC
SNOWFLAKE_SCHEMA_BRONZE=RAW
SNOWFLAKE_SCHEMA_SILVER=CURATED
SNOWFLAKE_SCHEMA_GOLD=BUSINESS

# Rutas
CONFIG_PATH=config/config.yaml
```

### 2. Archivo de Configuración (`config/config.yaml`)

```
ingestion:
  sources:
    - id: eu_superstore
      path: ./data/Sample_EU.xlsx
      type: excel
      schema: RAW
      tables:
        - sheet: Orders
          name: ORDERS
          method: put_copy_into
        - sheet: Returns
          name: RETURNS
          method: write_pandas
        - sheet: People
          name: PEOPLE
          method: write_pandas
  settings:
    create_csv: true
    temp_dir: ./data/temp
```

---

## Ejecución

### Ejecutar Pipeline Completo

```
python3 scripts/main.py
```

**Esto ejecuta:**

1. **FASE 1**: Setup de infraestructura (warehouses, DB, schemas)
2. **FASE 2**: Ingesta de datos (XLSX -> CSV -> Snowflake)
3. **FASE 3**: Validación y limpieza (detección de duplicados)
4. **FASE 4**: Transformación a SILVER (enriquecimiento)
5. **FASE 5**: Creación de vistas GOLD (análisis)

### Logs

Los logs se escriben en consola con timestamps:

```
[02:43:34] - [INFO] - Iniciando pipeline...
[02:43:43] - [INFO] - Cambiando a warehouse: COMPUTE_WH
[02:43:46] - [INFO] - Archivo SQL ejecutado con éxito. 9 sentencias ejecutadas.
...
```

---

## Estructura del Proyecto

```
snowflake-poc/
├── config/
│   └── config.yaml                 # Configuración de ingesta
├── data/
│   ├── Sample_EU.xlsx              # Datos fuente
│   └── temp/                       # CSVs intermedios
├── scripts/
│   ├── main.py                     # Orquestador principal
│   ├── ingest.py                   # Lógica de ingesta (PUT, write_pandas)
│   ├── convert_xlsx_to_csv.py      # Conversión Excel a CSVs
│   └── validation_cleansing.py     # Validación y deduplicación
├── sql/
│   ├── 01_setup_infra.sql          # Warehouses, DB, schemas
│   ├── 02_create_raw_tables.sql    # Tablas en capa RAW
│   ├── 03_transform_curated.sql    # Transformaciones a SILVER
│   └── 04_build_business_views.sql # Vistas GOLD
├── .env.example                    # Template de variables
├── requirements.txt                # Dependencias Python
└── README.md                       # Este archivo
```

---

## Vistas de Análisis

### 1. Órdenes, Ventas y Ganancia por Año (Pregunta 1)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_ORDERS_SALES_PROFIT_BY_YEAR;
```

![V_ORDERS_SALES_PROFIT_BY_YEAR](snowflake-poc/images/v1.jpg "ORDERS SALES PROFIT BY YEAR")

**Métricas:** Total de órdenes, clientes únicos, ventas, ganancia, margen, crecimiento

### 2. Órdenes por Día de la Semana 2017 (Pregunta 2)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_ORDERS_BY_DAY_OF_WEEK_2017;
```

![V_ORDERS_BY_DAY_OF_WEEK_2017](snowflake-poc/images/v2.jpg "ORDERS BY DAY OF WEEK 2017")

**Insights:** Viernes es el día más fuerte (22% del volumen), Jueves muestra anomalía (-90%)

### 3. Ventas por Segmento (Pregunta 3)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_SALES_BY_SEGMENT_ALL_YEARS;
```

![V_SALES_BY_SEGMENT_ALL_YEARS](snowflake-poc/images/v3.jpg "SALES BY SEGMENT ALL YEARS")

**Análisis:** Desempeño por segmento (Consumer, Corporate, Home Office) con margen por año

### 4. Top 10 Productos 2017 (Pregunta 4)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_TOP_10_PRODUCTS_2017;
```

![V_TOP_10_PRODUCTS_2017](snowflake-poc/images/v4.jpg "TOP 10 PRODUCTS 2017")

### 5. Bottom 10 Productos 2015 (Pregunta 5)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_BOTTOM_10_PRODUCTS_2015;
```

![V_BOTTOM_10_PRODUCTS_2015](snowflake-poc/images/v5.jpg "BOTTOM 10 PRODUCTS 2015")

### 6. Órdenes Devueltas 2015 (Pregunta 6)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_RETURNED_ORDERS_2015;
```

![V_RETURNED_ORDERS_2015](snowflake-poc/images/v6.jpg "RETURNED ORDERS 2015")

### 7. Devoluciones por Gerente Regional 2015 (Pregunta 7)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_RETURNED_BY_MANAGER_2015;
```

![V_RETURNED_BY_MANAGER_2015](snowflake-poc/images/v7.jpg "RETURNED BY MANAGER 2015")

**Análisis Regional:**
- Central (Emily Burns): 6.24% tasa de devolución
- North (Ross DeVincentis): 5.53% tasa de devolución (mejor rentabilidad)
- South (Damaia Kotsonis): 4.85% tasa de devolución (mejor calidad)

### 8. KPIs de Desempeño Empresa (Pregunta 8)

```
SELECT * FROM BPOLABS_POC.BUSINESS.V_COMPANY_PERFORMANCE_KPI;
```

![V_COMPANY_PERFORMANCE_KPI_1](snowflake-poc/images/v8_1.jpg "COMPANY PERFORMANCE KPI")
![V_COMPANY_PERFORMANCE_KPI_2](snowflake-poc/images/v8_2.jpg "COMPANY PERFORMANCE KPI")

### Análisis Críticos Adicionales

**Rentabilidad de Productos:**
```
SELECT * FROM BPOLABS_POC.BUSINESS.V_PRODUCT_PROFITABILITY_ANALYSIS;
```

![V_PRODUCT_PROFITABILITY_ANALYSIS](snowflake-poc/images/va_1.jpg "PRODUCT PROFITABILITY ANALYSIS")

**Órdenes No Rentables:**
```
SELECT * FROM BPOLABS_POC.BUSINESS.V_UNPROFITABLE_ORDERS_ANALYSIS;
```

![V_UNPROFITABLE_ORDERS_ANALYSIS](snowflake-poc/images/va_2.jpg "UNPROFITABLE ORDERS ANALYSIS")

**Desempeño Regional:**
```
SELECT * FROM BPOLABS_POC.BUSINESS.V_REGIONAL_PERFORMANCE;
```

![V_REGIONAL_PERFORMANCE](snowflake-poc/images/va_3.jpg "REGIONAL PERFORMANCE")

---

## Validación de Datos

### Validaciones Automáticas Ejecutadas

1. **Archivo fuente existe:** Verifica ruta de Excel
2. **Tablas destino existen:** Before ingesta
3. **Duplicados detectados:** Por PK
4. **Integridad de columnas:** Validación de tipos
5. **Deduplicación segura:** Tabla auxiliar + transacciones

---

## Próximos Pasos

### Mejoras Planificadas

- [ ] Documentación de lineaje
- [ ] Integración con Apache Airflow para orquestación automática
- [ ] Migración a dbt para transformaciones SQL
- [ ] Dashboard en Tableau/Looker conectado a vistas

### Optimizaciones

- [ ] Incremental loads (cargar solo datos nuevos)
- [ ] Clustering automático en Snowflake
- [ ] Compresión de archivos en stages
- [ ] Monitoreo de costs en Snowflake

### Recomendaciones de Negocio

Basado en los datos analizados:

1. **Urgente:** Revisar 37.71% de productos no rentables
2. **Estrategia:** Implementar dynamic pricing para margen negativo
3. **Regional:** Replicar mejores prácticas de South region
4. **Operacional:** Investigar anomalía de jueves en 2017

---

## Contribuir

1. Fork el repositorio
2. Crear rama feature (`git checkout -b feature/mejora`)
3. Commit cambios (`git commit -am 'Add feature'`)
4. Push a rama (`git push origin feature/mejora`)
5. Abrir Pull Request

---

## Licencia

Este proyecto está bajo licencia MIT. Ver archivo `LICENSE` para detalles.

---

## Autor

**[Pablo Ratache Rojas]** - Data Engineer

- GitHub: [@Pr0nel](https://github.com/Pr0nel)
- LinkedIn: [Pablo Ratache](www.linkedin.com/in/pablo-ratache-rojas-9a9602140)
- Portfolio: [Pablo's Portfolio](https://pr0nel.github.io/cv_pablo_ratache/)

---

## Contacto & Soporte

Para reportar bugs o sugerir mejoras, abrir un [Issue](https://github.com/tu-usuario/snowflake-poc/issues).

---

## Changelog

### v1.0.0 (2025-10-23)
- Pipeline completo funcional
- 8 vistas de análisis principales
- Validación de datos
- Documentación completa
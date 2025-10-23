-- sql/01_setup_infra.sql

-- Warehouses para Arquitectura Medallion
-- =======================================

-- COMPUTE_WH: Para operaciones administrativas y setup
CREATE WAREHOUSE IF NOT EXISTS ${WH_COMPUTE} 
WITH 
  WAREHOUSE_SIZE = 'XSMALL'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  SCALING_POLICY = 'STANDARD'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Operaciones ligeras (DDL y setup)';

-- ANALYTICS_WH: Para procesamiento de datos
CREATE WAREHOUSE IF NOT EXISTS ${WH_ANALYTICS} 
WITH 
  WAREHOUSE_SIZE = 'SMALL'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'STANDARD'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Para ETL, transformaciones, y analytics';

-- Crear base de datos y esquemas
-- =======================================

-- Usar el warehouse de computo para setup
USE WAREHOUSE ${WH_COMPUTE};

-- Crear base de datos principal
CREATE DATABASE IF NOT EXISTS ${DB}
  -- DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Database para POC con arquitectura Medallion';

-- Cambiar al contexto de la nueva base de datos
USE DATABASE ${DB};

-- Crear todos los esquemas necesarios
CREATE SCHEMA IF NOT EXISTS ${DB}.${SCHEMA_BRONZE}
  COMMENT = 'Capa Bronze - Datos crudos sin transformar con metadata de ingesta';
CREATE SCHEMA IF NOT EXISTS ${DB}.${SCHEMA_SILVER}
  COMMENT = 'Capa Silver - Datos limpios, validados y transformados';
CREATE SCHEMA IF NOT EXISTS ${DB}.${SCHEMA_GOLD}
  COMMENT = 'Capa Gold - Vistas de negocio y agregaciones de métricas';

SELECT 
  CURRENT_WAREHOUSE() as warehouse,
  CURRENT_DATABASE() as database,
  CURRENT_SCHEMA() as schema_name;
-- 01_setup_infra.sql

-- Estrategia de Warehouses para Arquitectura Medallion
-- =====================================================

-- COMPUTE_WH: Para operaciones administrativas y setup
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
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
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH 
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
-- ==============================
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS RAPPI_POC
  -- DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Database para POC con arquitectura Medallion';
USE DATABASE RAPPI_POC;

-- Crear todos los esquemas necesarios
CREATE SCHEMA IF NOT EXISTS RAW
  -- DATA_RETENTION_TIME_IN_DAYS = 3
  COMMENT = 'Capa Bronze - Datos crudos sin transformar';
CREATE SCHEMA IF NOT EXISTS CURATED
  -- DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Capa Silver - Datos limpios y transformados';
CREATE SCHEMA IF NOT EXISTS BUSINESS
  -- DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Capa Gold - Vistas de negocio y agregaciones';

SELECT 
  CURRENT_WAREHOUSE() as warehouse,
  CURRENT_DATABASE() as database,
  CURRENT_SCHEMA() as schema_name;
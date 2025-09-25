-- 02_create_raw_tables.sql

USE DATABASE RAPPI_POC;
USE SCHEMA RAW;

-- Tabla para BankChurners
CREATE OR REPLACE TABLE BANK_CHURNERS_RAW (
    "clientnum" BIGINT,
    "attrition_flag" TEXT,
    "customer_age" BIGINT,
    "gender" TEXT,
    "dependent_count" BIGINT,
    "education_level" TEXT,
    "marital_status" TEXT,
    "income_category" TEXT,
    "card_category" TEXT,
    "months_on_book" BIGINT,
    "total_relationship_count" BIGINT,
    "months_inactive_12_mon" BIGINT,
    "contacts_count_12_mon" BIGINT,
    "credit_limit" FLOAT,
    "total_revolving_bal" BIGINT,
    "avg_open_to_buy" FLOAT,
    "total_amt_chng_q4_q1" FLOAT,
    "total_trans_amt" BIGINT,
    "total_trans_ct" BIGINT,
    "total_ct_chng_q4_q1" FLOAT,
    "avg_utilization_ratio" FLOAT
);

-- Tabla para Coffee Sales
CREATE OR REPLACE TABLE COFFEE_SALES_RAW (
    "order_id" TEXT,
    "order_date" DATE,
    "customer_id" TEXT,
    "product_id" TEXT,
    "quantity" BIGINT,
    "customer_name" TEXT,
    "email" TEXT,
    "country" TEXT,
    "coffee_type" TEXT,
    "roast_type" TEXT,
    "size" TEXT,
    "unit_price" FLOAT,
    "sales" FLOAT
);
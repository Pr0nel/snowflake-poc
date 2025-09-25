-- 04_build_business_views.sql

USE SCHEMA BUSINESS;

-- Reporte diario de ventas
CREATE OR REPLACE VIEW DAILY_SALES_REPORT AS
SELECT
    "order_date",
    "coffee_type",
    SUM("sales") AS "total_sales",
    COUNT(*) AS "transaction_count",
    AVG("unit_price") AS "avg_price"
FROM CURATED.COFFEE_SALES
GROUP BY "order_date", "coffee_type";

-- Riesgo de abandono de clientes
CREATE OR REPLACE VIEW CUSTOMER_CHURN_RISK AS
SELECT
    "clientnum",
    "customer_age",
    "income_category",
    "total_trans_ct",
    "avg_utilization_ratio",
    CASE
        WHEN "avg_utilization_ratio" < 0.1 AND "months_inactive_12_mon" > 2 THEN 'High Risk'
        WHEN "avg_utilization_ratio" < 0.2 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS "churn_risk_level"
FROM CURATED.BANK_CUSTOMERS;
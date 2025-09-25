-- 03_transform_curated.sql

USE SCHEMA CURATED;

-- Tabla de clientes bancarios
CREATE OR REPLACE TABLE BANK_CUSTOMERS AS
SELECT
    "clientnum",
    CASE WHEN "attrition_flag" = 'Existing Customer' THEN 0 ELSE 1 END AS is_churned,
    "customer_age",
    "gender",
    "dependent_count",
    "education_level",
    "marital_status",
    "income_category",
    "card_category",
    "months_on_book",
    "total_relationship_count",
    "months_inactive_12_mon",
    "contacts_count_12_mon",
    "credit_limit",
    "total_revolving_bal",
    "avg_open_to_buy",
    "total_amt_chng_q4_q1",
    "total_trans_amt",
    "total_trans_ct",
    "total_ct_chng_q4_q1",
    "avg_utilization_ratio"
FROM RAPPI_POC.RAW.BANK_CHURNERS_RAW;

-- Tabla de ventas de cafe
CREATE OR REPLACE TABLE COFFEE_SALES AS
SELECT
    "order_id",
    "order_date",
    "customer_id",
    -- Extraer tipo de cafe
    CASE 
        WHEN SUBSTRING("product_id", 1, 1) = 'R' THEN 'Regular'
        WHEN SUBSTRING("product_id", 1, 1) = 'E' THEN 'Espresso'
        WHEN SUBSTRING("product_id", 1, 1) = 'L' THEN 'Latte'
        WHEN SUBSTRING("product_id", 1, 1) = 'A' THEN 'Americano'
        WHEN SUBSTRING("product_id", 1, 1) = 'M' THEN 'Mocha'
        ELSE 'Unknown'
    END AS "coffee_type",
    -- Extraer tipo de tostato
    CASE 
        WHEN SUBSTRING("product_id", 2, 1) = 'S' THEN 'Light'
        WHEN SUBSTRING("product_id", 2, 1) = 'M' THEN 'Medium'
        WHEN SUBSTRING("product_id", 2, 1) = 'D' THEN 'Dark'
        ELSE 'Unknown'
    END AS "roast_type",
    -- Extraer tamanio en onzas
    TRY_CAST(SUBSTRING("product_id", 3) AS FLOAT) AS "size",
    -- Estimar precio unitario
    COALESCE("unit_price", 
        CASE 
            WHEN SUBSTRING("product_id", 1, 1) = 'R' THEN 5.0
            WHEN SUBSTRING("product_id", 1, 1) = 'E' THEN 3.0
            WHEN SUBSTRING("product_id", 1, 1) = 'L' THEN 6.0
            WHEN SUBSTRING("product_id", 1, 1) = 'A' THEN 4.5
            WHEN SUBSTRING("product_id", 1, 1) = 'M' THEN 6.5
            ELSE 5.0
        END
    ) AS "unit_price",
    -- Calcular Resultado: Sales
    COALESCE("unit_price", 
        CASE 
            WHEN SUBSTRING("product_id", 1, 1) = 'R' THEN 5.0
            WHEN SUBSTRING("product_id", 1, 1) = 'E' THEN 3.0
            WHEN SUBSTRING("product_id", 1, 1) = 'L' THEN 6.0
            WHEN SUBSTRING("product_id", 1, 1) = 'A' THEN 4.5
            WHEN SUBSTRING("product_id", 1, 1) = 'M' THEN 6.5
            ELSE 5.0
        END
    ) * "quantity" AS "sales"
FROM RAW.COFFEE_SALES_RAW
WHERE "order_id" IS NOT NULL
AND "order_date" IS NOT NULL;
-- sql/02_create_raw_tables.sql
USE DATABASE ${DB};
USE SCHEMA ${SCHEMA_BRONZE};

-- Tabla para Orders
CREATE OR REPLACE TABLE ${TABLE_01} (
    ROW_ID          INTEGER         COMMENT 'Identificador único de fila',
    ORDER_ID        VARCHAR(50)     COMMENT 'ID único del pedido',
    ORDER_DATE      DATE            COMMENT 'Fecha del pedido',
    SHIP_DATE       DATE            COMMENT 'Fecha de envío',
    SHIP_MODE       VARCHAR(20)     COMMENT 'Modo de envío',
    CUSTOMER_ID     VARCHAR(15)     COMMENT 'ID del cliente',
    CUSTOMER_NAME   VARCHAR(50)     COMMENT 'Nombre del cliente',
    SEGMENT         VARCHAR(50)     COMMENT 'Segmento del cliente',
    CITY            VARCHAR(50)     COMMENT 'Ciudad del cliente',
    STATE           VARCHAR(50)     COMMENT 'Estado/provincia del cliente',
    COUNTRY         VARCHAR(50)     COMMENT 'País del cliente',
    REGION          VARCHAR(50)     COMMENT 'Región geográfica',
    PRODUCT_ID      VARCHAR(50)     COMMENT 'ID del producto',
    CATEGORY        VARCHAR(50)     COMMENT 'Categoría del producto',
    SUB_CATEGORY    VARCHAR(50)     COMMENT 'Subcategoría del producto',
    PRODUCT_NAME    VARCHAR(100)    COMMENT 'Nombre del producto',
    SALES           NUMBER(10, 2)   COMMENT 'Ventas totales',
    QUANTITY        INTEGER         COMMENT 'Cantidad vendida',
    DISCOUNT        NUMBER(5, 2)    COMMENT 'Descuento aplicado',
    PROFIT          NUMBER(10, 2)   COMMENT 'Ganancia obtenida'
);

-- Tabla para Returns
CREATE OR REPLACE TABLE ${TABLE_02} (
    ORDER_ID    VARCHAR(50) COMMENT 'ID del pedido',
    RETURNED    VARCHAR(3)  COMMENT 'Indica si el pedido fue devuelto (Yes/No)'
);

-- Tabla para People
CREATE OR REPLACE TABLE ${TABLE_03} (
    REGION  VARCHAR(50)     COMMENT 'Región geográfica',
    PEOPLE  VARCHAR(100)    COMMENT 'Nombre de la persona asociada a la región'
);
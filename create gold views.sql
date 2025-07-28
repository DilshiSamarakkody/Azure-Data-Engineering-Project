/*
    CREATE VIEW CALENDER
*/
CREATE VIEW gold.calender
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Calender/',
                FORMAT = 'PARQUET'
              ) AS QUERY01

/*
    CREATE VIEW CUSTOMERS
*/
CREATE VIEW gold.customers
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Customers/',
                FORMAT = 'PARQUET'
              ) AS QUERY01

/*
    CREATE VIEW PRODUCT CATEGORIES
*/
CREATE VIEW gold.product_categories
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Product_Categories/',
                FORMAT = 'PARQUET'
              ) AS QUERY01

/*
    CREATE VIEW PRODUCT SUBCATEGORIES
*/
CREATE VIEW gold.product_sub_categories
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
                FORMAT = 'PARQUET'
              ) AS QUERY01

/*
    CREATE VIEW PRODUCTS
*/
CREATE VIEW gold.products
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Products/',
                FORMAT = 'PARQUET'
              ) AS QUERY01

/*
    CREATE VIEW RETURNS
*/
CREATE VIEW gold.returns
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Returns/',
                FORMAT = 'PARQUET'
              ) AS QUERY01


/*
    CREATE VIEW TERRITORIES
*/
CREATE VIEW gold.territories
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Territories/',
                FORMAT = 'PARQUET'
              ) AS QUERY01

/*
    CREATE VIEW SALES
*/
CREATE VIEW gold.sales
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_sales/',
                FORMAT = 'PARQUET'
              ) AS QUERY01















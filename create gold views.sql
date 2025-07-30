/*
    CREATE VIEW DATES
*/
CREATE VIEW gold.dim_dates
AS
SELECT 
ROW_NUMBER() OVER(ORDER BY Date) AS Datekey,
Date,
Month,
Year
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Dates/',
                FORMAT = 'PARQUET'
              ) AS DATES

GO 
/*
    CREATE VIEW CUSTOMERS
*/
CREATE VIEW gold.dim_customers
AS
SELECT
ROW_NUMBER() OVER (ORDER BY CustomerKey) AS CustomerKey,
CustomerKey AS CustomerID,
Prefix,
FirstName,
LastName,
BirthDate,
MaritalStatus,
Gender,
EmailAddress,
AnnualIncome,
TotalChildren,
EducationLevel,
Occupation,
HomeOwner,
FullName,
Age
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Customers/',
                FORMAT = 'PARQUET'
              ) AS CUSTOMERS

GO
/*
    CREATE VIEW PRODUCTS
*/
CREATE VIEW gold.dim_products
AS
SELECT 
ROW_NUMBER() OVER(ORDER BY productID) AS ProductKey,
p.productID,
p.productSku,
p.productName,
p.ModelName,
p.ProductDescription,
cat.CategoryName AS ProductCategory,
sub.SubcategoryName AS ProductSubCategory,
p.ProductColor,
p.ProductStyle,
p.ProductCost,
p.ProductPrice
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Products/',
                FORMAT = 'PARQUET'
              ) AS p
LEFT JOIN 
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
                FORMAT = 'PARQUET'
              ) AS sub
    ON p.ProductSubcategoryKey = sub.ProductSubcategoryKey 
LEFT JOIN 
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Product_Categories/',
                FORMAT = 'PARQUET'
              ) AS cat
    ON sub.ProductCategoryKey = cat.ProductcategoryKey
GO
/*
    CREATE VIEW TERRITORIES
*/
CREATE VIEW gold.dim_territories
AS
SELECT * 
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Territories/',
                FORMAT = 'PARQUET'
              ) AS ter

GO

/*
    CREATE VIEW FACT_SALES
*/
CREATE VIEW gold.fact_sales
AS
SELECT 
s.OrderNumber,
s.OrderDate,
s.OrderLineItem,
s.OrderQuantity,
s.StockDate,
p.ProductKey,
c.CustomerKey,
t.TerritoryKey,
d.Datekey AS OrderDateKey,
r.ReturnDate,
r.ReturnQuantity
FROM
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Sales/',
                FORMAT = 'PARQUET'
              ) AS s
LEFT JOIN
    OPENROWSET
             (
                BULK 'https://adventureworksstorageacc.blob.core.windows.net/silver/AdventureWorks_Returns/',
                FORMAT = 'PARQUET'
              ) AS r
    ON s.Productkey = r.ProductKey
    AND s.TerritoryKey = r.TerritoryKey
LEFT JOIN gold.dim_customers c
    ON s.CustomerKey = c.CustomerID
LEFT JOIN gold.dim_products p
    ON s.ProductKey = p.productID
LEFT JOIN gold.dim_territories t
    ON s.TerritoryKey = t.TerritoryKey
LEFT JOIN gold.dim_dates d
    ON s.OrderDate = d.Date

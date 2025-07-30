CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Dilshi@2000'

CREATE DATABASE SCOPED CREDENTIAL cred_dilshi
WITH
    IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE source_silver
WITH
    (
        LOCATION = 'https://adventureworksstorageacc.blob.core.windows.net/silver',
        CREDENTIAL = cred_dilshi
    )

CREATE EXTERNAL DATA SOURCE source_gold
WITH
    (
        LOCATION = 'https://adventureworksstorageacc.blob.core.windows.net/gold',
        CREDENTIAL = cred_dilshi
    )

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
    (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    )


------------------------------------------------
----CREATE EXTERNAL TABLE EXTDIMDATES-----------
------------------------------------------------
CREATE EXTERNAL TABLE gold.extdimdates
WITH
(
    LOCATION = 'extdimdates',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.dim_dates


-------------------------------------------------
----CREATE EXTERNAL TABLE EXTDIMCUSTOMER---------
-------------------------------------------------
CREATE EXTERNAL TABLE gold.extdimcustomer
WITH
(
    LOCATION = 'extdimcustomer',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.dim_customers



-------------------------------------------------
----CREATE EXTERNAL TABLE EXTDIMPRODUCT ---------
-------------------------------------------------
CREATE EXTERNAL TABLE gold.extdimproducts
WITH
(
    LOCATION = 'extdimproducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.dim_products


-------------------------------------------------
----CREATE EXTERNAL TABLE TERRITORIES -----------
-------------------------------------------------
CREATE EXTERNAL TABLE gold.extdimterritories
WITH
(
    LOCATION = 'extdimterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.dim_territories

------------------------------------------------
----CREATE EXTERNAL TABLE EXTFACTSALES----------
------------------------------------------------
CREATE EXTERNAL TABLE gold.extfactsales
WITH
(
    LOCATION = 'extfactsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS SELECT * FROM gold.fact_sales














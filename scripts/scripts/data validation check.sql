/* Data Check Script */

-- Checking for invalid Date Orders (EXP: None)
SELECT*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- Data Consistency between sales, quantity, and price. (Bussiness Rule provided: They should not be negative, zero or nulls)
-- //Bussiness Rule provided: 
-- * They should not be negative, zero or nulls
-- * sales = quantity*price
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL;

USE DataWarehouse;
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price OR sls_sales IS NULL OR sls_quantity IS NULL 
OR sls_price IS NULL OR sls_sales <=0 OR sls_quantity <= 0 or sls_price <=0;

-- > Business rules provided to handle errors: 
-- * If sales is negative, zero or null, derive it using Quantity and price (sales = quantity*price)
-- * If price is null, derive it from sales and quantity (price = sales/quantity)
-- * If quantity is null,derive from sales and price (quantity = sales/price)
-- * If price is negative, convert to positive value (ABS(price))



select 
sls_sales as old_sales,
sls_quantity as old_quant,
sls_price as old_price,
CASE
WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) THEN sls_quantity * ABS(sls_price)
ELSE sls_sales
END AS sls_sales,
CASE
WHEN sls_quantity IS NULL THEN sls_sales/ NULLIF (sls_price, 0)
ELSE sls_quantity
END AS sls_quantity,
CASE
WHEN sls_price < 0 THEN ABS(sls_price)
ELSE sls_price
END AS sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity*sls_price or sls_sales is null;


EXECUTE sp_help 'silver.crm_sales_details'

DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

select *
from silver.crm_sales_details;

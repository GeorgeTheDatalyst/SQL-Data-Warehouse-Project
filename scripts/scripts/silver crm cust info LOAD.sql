INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
-- rest of your query here

select
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	ELSE 'N/A'
	END cst_marital_status,
CASE 
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	ELSE 'OTHER'
END cst_gndr,
cst_create_date
from
(
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as row_date_rank
from bronze.crm_cust_info
) as t where row_date_rank = 1 and cst_id is not null;

select*
from silver.crm_cust_info;
/*
=========================================
Create Dimention: gold.dim_customers
=========================================
*/

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER () OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_marital_status AS marital_status,
	CASE
	WHEN ci.cst_gndr != 'OTHER' THEN ci.cst_gndr
	ELSE coalesce (ca.GEN,'other')
	END AS gender,
	ca.BDATE AS birth_date,
	ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_CUST_AZ12 ca ON ci.cst_key=ca.CID
	LEFT JOIN silver.erp_LOC_A101 la ON ci.cst_key=la.CID;

	

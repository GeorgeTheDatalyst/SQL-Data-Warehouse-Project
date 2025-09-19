/*
======================================
Create Dimention: gold.dim_products
======================================
*/

CREATE VIEW gold.dim_products AS
SELECT DISTINCT
ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) AS product_key,
pr.prd_id AS product_id,
pr.prd_key AS product_number,
pr.prd_nm AS product_name,
pr.cat_ID AS category_id,
px.CAT AS category,
px.SUBCAT AS sub_category,
px.MAINTENANCE AS maintenance,
pr.prd_line AS product_line,
pr.prd_cost AS product_cost,
pr.prd_start_dt AS product_start_date,
pr.sls_prd_key AS sales_product_number

FROM silver.crm_prd_info pr
LEFT JOIN silver.erp_PX_CAT_G1V2 px ON pr.cat_ID=px.ID
WHERE pr.prd_end_dt is null; ----------------------------------- > Filtered out historical data


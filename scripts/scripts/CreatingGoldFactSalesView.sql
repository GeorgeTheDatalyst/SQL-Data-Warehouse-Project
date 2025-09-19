/*
=====================================
Creata FactTable: gold.fact_sales
=====================================
*/

CREATE VIEW gold.fact_sales AS
select
sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipment_date,
sls_due_dt AS due_date,
sls_sales AS sales_amount,
sls_quantity AS quantity,
sls_price AS price
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key=pr.sales_product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id=cu.customer_id;



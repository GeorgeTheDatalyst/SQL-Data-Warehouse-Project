# 🟡 View: `gold.dim_products`

## 📌 Purpose
The `gold.dim_products` view defines the **product dimension** in the gold layer of the data warehouse. 
  It enriches CRM product data with ERP category and maintenance attributes, 
  and filters out historical products to ensure only active records are used in reporting and analytics.

---

## 🧾 SQL Definition

```sql
CREATE VIEW gold.dim_products AS
SELECT DISTINCT
    ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) AS product_key,
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
LEFT JOIN silver.erp_PX_CAT_G1V2 px ON pr.cat_ID = px.ID
WHERE pr.prd_end_dt IS NULL; -- Filtered out historical data


# 🟡 View: `gold.dim_customers`

## 📌 Purpose
The `gold.dim_customers` view defines the **customer dimension** in the gold layer of the data warehouse. 
  It enriches CRM customer data with demographic and location details from ERP sources, 
  creating a unified customer profile for analytics and reporting.

---

## 🧾 SQL Definition

```sql
CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.CNTRY AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'OTHER' THEN ci.cst_gndr
        ELSE COALESCE(ca.GEN, 'other')
    END AS gender,
    ca.BDATE AS birth_date,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_CUST_AZ12 ca ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 la ON ci.cst_key = la.CID;


# 💰 View: `gold.fact_sales`

## 📌 Purpose
The `gold.fact_sales` view defines the **sales fact table** in the gold layer of the data warehouse. 
  It captures transactional sales data and enriches it with dimensional keys from `dim_products` and `dim_customers`,
  enabling robust analytical queries and reporting.

  🧠 Usage Notes
This view is designed for use in star schema models and BI dashboards.
Joins are performed using LEFT JOIN to preserve sales records even if dimensional data is incomplete.
Surrogate keys (product_key, customer_key) support efficient joins and aggregations.

---

## 🧾 SQL Definition

```sql
CREATE VIEW gold.fact_sales AS
SELECT
    sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sls_order_dt AS order_date,
    sls_ship_dt AS shipment_date,
    sls_due_dt AS due_date,
    sls_sales AS sales_amount,
    sls_quantity AS quantity,
    sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.sales_product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id;



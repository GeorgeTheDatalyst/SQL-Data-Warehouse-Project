# 📊 CRM Sales Details ETL & Validation Scripts

## Overview
This repository contains SQL scripts for validating and transforming CRM sales data from the `bronze.crm_sales_details` table into the `silver.crm_sales_details` table. It ensures data integrity by applying business rules and correcting inconsistencies before loading into the silver layer.

---

## Architecture

- **Bronze Layer**: Raw sales data with potential inconsistencies
- **Silver Layer**: Cleaned, validated, and enriched sales data
- **Validation Layer**: Pre-checks to enforce business rules before transformation

---

## 🔍 Data Validation Script

Before running the ETL, the following checks are performed to identify and isolate invalid records:

### 1. **Invalid Date Orders**
Ensures that order dates precede ship and due dates.

```sql
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

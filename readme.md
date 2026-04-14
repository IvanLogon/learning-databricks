# Retail Sales Analytics & Governance Platform

A data engineering solution implementing **medallion architecture** (Bronze → Silver → Gold) with **security governance** using Unity Catalog's Attribute-Based Access Control (ABAC) for retail sales analytics on Databricks.

## 🎯 Overview

This workspace demonstrates a complete end-to-end data platform featuring:

* **Multi-layer data pipeline** with automated data quality and transformation
* **Column-level security** using governed tags and ABAC policies
* **Role-based access control** for sensitive financial data
* **Incremental data processing** for efficient pipeline execution
* **Unity Catalog governance** with tag-based data classification

## 📊 Dataset

Built on the [Retail Store Sales Dataset](https://www.kaggle.com/datasets/ahmedmohamed2003/retail-store-sales-dirty-for-data-cleaning/data) from Kaggle - intentionally dirty transactional data designed for data engineering practice.

## 🏗️ Architecture

### Data Pipeline: Medallion Architecture

```
Raw CSV Files → Bronze Layer → Silver Layer → Gold Layer
    ↓              ↓              ↓              ↓
  Volume      Raw Ingestion  Data Quality   Analytics
              (Full/Incremental) (Validation) (Aggregates)
```

**Bronze Layer** (`sales.bronze.*`)
* Two loading strategies: **Full Load** and **Incremental Load**
* Schema enforcement and column standardization
* Raw data preservation with minimal transformation
* Tables: `retail_store_sales`

**Silver Layer** (`sales.silver.*`)
* Data quality validation and cleansing
* Business rule application and imputation
* Type casting and data enrichment
* Tables: `retail_store_sales`

**Gold Layer** (`sales.gold.*`)
* Business-ready aggregated datasets
* Pre-computed metrics for analytics
* Customer dimension with synthetic demographics
* Optimized for BI consumption
* Tables: `sales_fact`, `customers_dim`, `category_dim`, `payment_dim`

### Bronze Layer: Two Loading Strategies

#### Full Load

**Notebook**: `full_bronze_retail_store_sales.ipynb`

**Approach:**
* Loads the entire CSV file on each run
* Overwrites the bronze table completely

#### Incremental Load

**Notebook**: `incremental_bronze_retail_store_sales.ipynb`

**Approach:**
* Loads only new records based on `transaction_date` watermark
* Appends to existing bronze table using MERGE
* Deduplicates using `transaction_id`

### Security: ABAC Governance Framework

```
Governed Tags → Column Tagging → ABAC Policies → Runtime Enforcement
      ↓               ↓                ↓                  ↓
  Tag Registry   Metadata Layer   Policy Engine    Query Execution
```

**Key Components:**
* **Governed Tag**: `financial_sensitivity` (values: high, medium, low)
* **Column Masks**: Dynamic data masking based on user group membership
* **Access Policies**: Catalog-level policies that automatically apply to tagged columns
* **User Exemptions**: Finance team and admins see unmasked data

## 📁 Project Structure

```
home/
├── readme.md
├── alerts/
│   └── Drops from bronze to silver        # Data quality monitoring alert
├── queries/
│   ├── mask.dbquery.ipynb                 # SQL: ABAC policy setup
│   └── analytics.dbquery.ipynb            # SQL: Aggregations on gold tables
├── notebooks/
│   ├── full_bronze_retail_store_sales     # Bronze: Full load
│   ├── incremental_bronze_retail_store_sales  # Bronze: Incremental load
│   ├── full_silver_retail_store_sales     # Silver: Data quality transformations
│   ├── full_gold_retail_store_sales       # Gold: Business aggregations
│   └── full_gold_synthetic                # Gold: Customer dimension with synthetic data
└── jobs/
    └── etl.yml            # Automated workflow (daily schedule)

Data Location:
└── /Volumes/sales/default/raw/retail_store_sales.csv
```

**Job Workflow**: The ETL job orchestrates the complete medallion pipeline with sequential task dependencies, running daily to refresh all layers from raw data to final analytics tables.

## 🚀 Setup & Configuration

### 1. Create Unity Catalog Structure

```sql
CREATE CATALOG IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS sales.default;
CREATE SCHEMA IF NOT EXISTS sales.bronze;
CREATE SCHEMA IF NOT EXISTS sales.silver;
CREATE SCHEMA IF NOT EXISTS sales.gold;
```

### 2. Upload Raw Data

Place the CSV file in Unity Catalog Volume:
```
/Volumes/sales/default/raw/retail_store_sales.csv
```

### 3. Create Governed Tag (via UI)

1. Navigate to **Catalog > Governed Tags**
2. Click **Create governed tag**
3. Set **Tag key**: `financial_sensitivity`
4. Set **Allowed values**: `high`, `medium`, `low`
5. Save the tag definition

### 4. Create User Groups

Create the following groups in Account Console:
* `finance_team` - Users who can see unmasked financial data
* `account users` - All users (default group)

Add appropriate users to `finance_team` for unrestricted access.

## References

* [Dataset Source](https://www.kaggle.com/datasets/ahmedmohamed2003/retail-store-sales-dirty-for-data-cleaning/data)
* [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
* [Delta Lake Documentation](https://docs.delta.io/)
* [Unity Catalog ABAC Policies](https://docs.databricks.com/data-governance/unity-catalog/abac/)
* [Governed Tags Documentation](https://docs.databricks.com/admin/governed-tags/)

---

**Platform**: Databricks on AWS  
**Last Updated**: April 2026  
**License**: MIT
# DWH_UM
# Bicycle Retail Data Warehouse Project

## Project Overview
This project aims to consolidate fragmented data from an **ERP** system and a **CRM** system into a centralized **SQL Server Data Warehouse**. The goal is to eliminate data silos and provide a reliable foundation for business intelligence.

## Data Warehouse Architecture
The project follows a three-layer architecture based on the **Kimball Methodology**:
1. **Ingestion Layer (Raw)**: Pulling 6 CSV files from source systems.
2. **Transformation Layer (Clean)**: Resolving inconsistencies, handling missing values, and generating surrogate keys.
3. **Curated Layer (Analytical)**: A Star Schema designed for reporting.

## Project Setup & Initialisation

### 1. Ingestion Strategy
- **Method**: Pull-based Batch processing (Day-to-day operation).
- **Naming Convention**: `source_entity` (snake_case), e.g., `crm_cust_info`.

### 2. Data Quality Issues & Transformations (Critical Fixes)
During exploration, we identified several inconsistencies that are resolved in the Transformation Layer:
- **Identity Management**: Generated **Surrogate Keys** because source IDs are missing or inconsistent.
- **Product Key Reconstruction**: Merged the first half of the key from `PXCAT` (ERP) with the second half from `sales_details` (CRM).
- **Data Cleaning**:
    - Trimmed white spaces in `firstname` and `lastname`.
    - Handled **Year 9999** and future birth dates in ERP.
    - Standardized Gender ("Male"/"M") and Country formats.
    - Filtered negative sales and prices in `sales_details`.
- **Date Logic**: Corrected records where `start_date > end_date`.

### 3. Data Modeling (Curated Layer)
- **Model**: Star Schema.
- **Tables**:
    - `fact_sales`: Transactional data with revenue and quantity.
    - `dim_customer`: Integrated demographics from both ERP and CRM.
    - `dim_products`: Merged product master and categories.
    - `dim_date`: Calendar dimension for time-series analysis.

## Naming Conventions
To maintain consistency and readability across the SQL Server environment, we strictly follow these naming rules:

### 1. General Formatting
- **Case Style**: `snake_case` (All lowercase, words separated by underscores).
- **Language**: All identifiers must be in English.

### 2. Layer-Specific Naming
- **Ingestion Layer (Staging)**: 
  - Pattern: `source_entity`
  - Examples: `erp_cust_az12`, `crm_cust_info`, `crm_sales_details`.
- **Transformation Layer**: 
  - Pattern: `int_entity` (Integrated)
  - Examples: `int_customer_cleaned`, `int_product_reconstructed`.
- **Curated Layer (Star Schema)**:
  - **Dimensions**: Prefix with `dim_` (e.g., `dim_customer`, `dim_products`).
  - **Facts**: Prefix with `fact_` (e.g., `fact_sales`).

## Documentation
The initial plan diagram can be found in the `/docs` folder.

# Takeout DBT Project

This is a small **DBT project** for transforming Google Takeout history data ingested into Snowflake.

---

## Overview

- Transforms raw Takeout data (YouTube, Chrome, Phone history, etc.) into structured tables.  
- Supports **staging** and **mart** models:  
  - **Staging models** – Clean and standardize raw data.  
  - **Mart models** – Ready-to-use datasets for analysis and reporting.  
- Table design was motivated by Metabase usage.  
- The dataset created by the topic modeling pipeline can also be used as input to the mart models.

---

## Usage

1. Ensure your raw Takeout data is loaded into Snowflake.  
2. Run DBT transformations:  
   ```bash
   dbt run

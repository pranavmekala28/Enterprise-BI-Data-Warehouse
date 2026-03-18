# 🏭 Enterprise BI & Data Warehouse — Supply Chain Analytics

![Pipeline](https://img.shields.io/badge/Pipeline-Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow)
![Database](https://img.shields.io/badge/Database-PostgreSQL-4169E1?style=for-the-badge&logo=postgresql)
![BI](https://img.shields.io/badge/BI-Power%20BI-F2C811?style=for-the-badge&logo=powerbi)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python)
![SQL](https://img.shields.io/badge/SQL-Advanced-orange?style=for-the-badge)

> **End-to-end Data Engineering project** simulating a real enterprise data warehouse pipeline — from messy SAP ERP exports to a production-ready star schema with automated orchestration and Power BI KPI dashboards.

---

## 📐 Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────────┐
│  SAP ERP Export │────▶│  Python ETL  │────▶│   PostgreSQL DW     │
│  (Raw CSV)      │     │  + DQ Checks │     │   (Star Schema)     │
└─────────────────┘     └──────────────┘     └──────────┬──────────┘
                               │                         │
                        ┌──────▼──────┐         ┌───────▼────────┐
                        │   Airflow   │         │    Power BI    │
                        │  Orchestr.  │         │   Dashboard    │
                        └─────────────┘         └────────────────┘
```

---

## 🗂️ Star Schema Design

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (1,461)    │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼──────┐    ┌──────────────┐
│ dim_customer │────│ fact_sales  │────│ dim_product  │
│    (300)     │    │   (2,000)   │    │    (200)     │
└──────────────┘    └──────┬──────┘    └──────────────┘
                           │
              ┌────────────┴────────────┐
       ┌──────▼──────┐         ┌────────▼─────┐
       │ dim_vendor  │         │  dim_plant   │
       │    (50)     │         │    (12)      │
       └─────────────┘         └──────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Orchestration** | Apache Airflow | Schedule & monitor ETL pipeline |
| **Database** | PostgreSQL 18 | Star schema data warehouse |
| **Transformation** | Python + Pandas | Data cleaning & modeling |
| **BI / Reporting** | Power BI Desktop | KPI dashboards & DAX measures |
| **Data Quality** | Custom Python DQ framework | 13 automated checks |
| **Version Control** | Git + GitHub | Code management |

---

## 📁 Project Structure

```
enterprise-bi-data-warehouse/
│
├── 📂 dags/
│   └── supply_chain_etl_pipeline.py   # Airflow DAG (6 tasks)
│
├── 📂 sql/
│   ├── 01_create_schema.sql           # Schema + table DDL
│   ├── 02_dim_tables.sql              # All 5 dimension tables
│   ├── 03_fact_table.sql              # Fact table with constraints
│   ├── 04_transformations.sql         # Staging → DW CTEs
│   ├── 05_yoy_revenue.sql             # Window function analysis
│   ├── 06_fulfillment_rate.sql        # Regional KPI query
│   ├── 07_customer_ltv.sql            # Customer lifetime value
│   └── 08_vendor_scorecard.sql        # Vendor performance
│
├── 📂 python/
│   ├── generate_raw_data.py           # SAP ERP simulation
│   ├── etl_transform.py               # Transformation logic
│   └── data_quality.py               # 13 DQ check framework
│
├── 📂 data/
│   ├── stg_erp_raw.csv                # Raw SAP export (2,000 rows)
│   ├── fact_sales.csv                 # Cleaned fact table
│   ├── dim_customer.csv               # Customer dimension
│   ├── dim_date.csv                   # Date dimension (1,461 rows)
│   ├── dim_product.csv                # Product dimension
│   ├── dim_vendor.csv                 # Vendor dimension
│   └── dim_plant.csv                  # Plant dimension
│
├── 📂 powerbi/
│   └── supply_chain_dashboard.pbix    # Power BI report file
│
├── 📂 docs/
│   └── data_dictionary.md             # Column definitions
│
└── README.md
```

---

## ⚙️ Airflow Pipeline (6 Tasks)

```
extract_and_validate
        │
        ▼
run_data_quality_checks   ◀── 13 automated checks
        │                     Fails if critical checks don't pass
        ▼
transform_data            ◀── Clean, dedupe, model
        │
        ▼
load_to_postgres          ◀── Truncate + reload all tables
        │
        ▼
validate_load             ◀── Row count assertions
        │
        ▼
notify_pipeline_complete
```

**Pipeline runs daily at 6:00 AM.**
Retries 2x on failure with 5-minute delay.

---

## 🔍 Data Quality Framework

| Check ID | Category | Description | Severity |
|---|---|---|---|
| DQ-001 | Completeness | No NULL ORDER_ID | Critical |
| DQ-002 | Completeness | No NULL CUSTOMER_ID | Critical |
| DQ-003 | Completeness | No NULL UNIT_PRICE_USD | High |
| DQ-004 | Completeness | No NULL QUANTITY | High |
| DQ-005 | Validity | UNIT_PRICE_USD >= 0 | High |
| DQ-006 | Validity | SHIP_DATE >= ORDER_DATE | High |
| DQ-007 | Validity | Valid FULFILLMENT_STATUS | High |
| DQ-008 | Uniqueness | ORDER_ID is unique | Critical |
| DQ-009 | Ref. Integrity | CUSTOMER_KEY resolves | Critical |
| DQ-010 | Ref. Integrity | PRODUCT_KEY resolves | Critical |
| DQ-011 | Business Rule | NET_REVENUE >= 0 | High |
| DQ-012 | Business Rule | DISCOUNT between 0-1 | Medium |
| DQ-013 | Completeness | No NULL MATERIAL_CODE | High |

---

## 📊 Power BI KPIs (DAX Measures)

```dax
-- Total Net Revenue
Total Net Revenue =
SUMX(fact_sales,
    fact_sales[quantity] * fact_sales[unit_price_usd] *
    (1 - fact_sales[discount_pct]))

-- YoY Revenue Growth
YoY Revenue Growth % =
DIVIDE([Total Net Revenue] - [Revenue LY], [Revenue LY])

-- Fulfillment Rate
Fulfillment Rate % =
DIVIDE(
    CALCULATE(COUNTROWS(fact_sales),
    fact_sales[fulfillment_status] = "Delivered"),
    COUNTROWS(fact_sales))
```

---

## 🚀 How to Run

### Prerequisites
```bash
pip install apache-airflow pandas numpy psycopg2-binary sqlalchemy
```

### 1. Start PostgreSQL and create schema
```bash
psql -U postgres -d postgres -f sql/01_create_schema.sql
```

### 2. Generate raw data
```bash
python python/generate_raw_data.py
```

### 3. Start Airflow
```bash
airflow db init
airflow webserver --port 8080 &
airflow scheduler &
```

### 4. Add PostgreSQL connection in Airflow UI
- Conn ID: `postgres_dw`
- Host: `localhost`
- Schema: `postgres`
- Login: `postgres`
- Port: `5432`

### 5. Trigger the DAG
```bash
airflow dags trigger supply_chain_etl_pipeline
```

### 6. Open Power BI
Connect to PostgreSQL → load all 6 tables → open `powerbi/supply_chain_dashboard.pbix`

---

## 📈 Sample Query Results

### YoY Revenue Growth by Quarter
```sql
-- Uses LAG() window function
SELECT year, quarter_name, net_revenue, yoy_growth_pct
FROM yoy_revenue_analysis
ORDER BY year, quarter_name;
```
| Year | Quarter | Net Revenue | YoY Growth |
|---|---|---|---|
| 2022 | Q1 | $4,823,441 | — |
| 2023 | Q1 | $5,102,873 | +5.8% |
| 2024 | Q1 | $5,441,209 | +6.6% |

---

## 💼 Skills Demonstrated

- ✅ **Dimensional Modeling** — Star schema design with fact & dimension tables
- ✅ **SQL** — Window functions, CTEs, aggregations, JOINs, MERGE/upsert
- ✅ **Python ETL** — pandas, data cleaning, transformation pipeline
- ✅ **Apache Airflow** — DAG design, task dependencies, error handling, retries
- ✅ **Data Quality** — Automated validation framework with severity levels
- ✅ **Power BI** — DAX measures, star schema relationships, KPI dashboards
- ✅ **PostgreSQL** — DDL, constraints, foreign keys, schema management
- ✅ **SAP/ERP Integration** — Simulated enterprise data export handling

---

## 👤 Author

**Pranav Mekala**
Data Engineer | BI Engineer
📧 Connect on [LinkedIn](#)

---

*This project simulates real enterprise data engineering patterns used at companies like Accenture, Amazon, and SAP consulting firms.*

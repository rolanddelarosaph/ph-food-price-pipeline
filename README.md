# 🇵🇭 Philippine Food Price Analytics Pipeline

> **Analytical Question:** Are food prices rising faster than inflation — and where are the largest gaps across regions and food categories?

[![Dashboard](https://img.shields.io/badge/Tableau-Live%20Dashboard-blue?logo=tableau)](https://public.tableau.com/app/profile/roland.dela.rosa/viz/PHFoodPriceDashboard/DASHBOARD)
[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://www.python.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-Transformations-FF694A?logo=dbt)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-E27B34?logo=apacheairflow)](https://airflow.apache.org/)

---

## 📊 Live Dashboard

[![Philippine Food Price Analytics Dashboard](docs/ph_food_price_dashboard.png)](https://public.tableau.com/app/profile/roland.dela.rosa/viz/PHFoodPriceDashboard/DASHBOARD)

> Click the image above to open the interactive dashboard

---

## 🏗️ Architecture

![Pipeline Architecture](docs/architecture.png)

---

## 📌 Project Overview

An end-to-end automated data pipeline that integrates three real-world Philippine data sources to analyze food price trends, inflation gaps, and purchasing power across all 17 Philippine regions from 2000 to 2023.

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) and is built entirely on modern cloud-native data engineering tools.

---

## 🔧 Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python 3.12 | Download, parse, and load raw data |
| Storage | Snowflake | Cloud data warehouse |
| Transformation | dbt | SQL-based data modeling and testing |
| Orchestration | Apache Airflow (Astronomer) | Monthly pipeline scheduling |
| Visualization | Tableau Public | Interactive dashboard |
| Architecture | Medallion (Bronze/Silver/Gold) | Layered data design pattern |

---

## 📂 Data Sources

| Source | Description | Format | Update Frequency |
|---|---|---|---|
| WFP OpenData | Actual market food prices across 17 PH regions | CSV | Manual (latest: Nov 2023) |
| PSA OpenSTAT | Official Consumer Price Index by region | Excel | Monthly |
| BSP API | Live USD/PHP exchange rate | JSON API | Daily (automated) |

---

## 🗂️ Project Structure

```
ph-food-pipeline/
├── dags/                          # Airflow DAG — monthly schedule
│   └── ph_food_pipeline_dag.py
├── data/
│   └── raw/                       # Source data files
│       ├── wfp_food_prices_phl.csv
│       ├── psa_cpi_2018_2020.xlsx
│       ├── psa_cpi_2021_2023.xlsx
│       └── psa_cpi_2024_2026.xlsx
├── docs/                          # Architecture diagrams and dashboard screenshots
│   ├── architecture.png
│   └── ph_food_price_dashboard.png
├── ph_food_pipeline/              # dbt project
│   ├── models/
│   │   ├── bronze/                # Raw ingestion models
│   │   ├── silver/                # Cleaned and typed models
│   │   └── gold/                  # Star schema — dimensions and fact table
│   ├── macros/
│   ├── tests/
│   └── dbt_project.yml
├── src/
│   ├── config.py                  # Snowflake connection config
│   └── ingest.py                  # Python ingestion functions
├── sql/
│   └── initialize_food_pipeline.sql
└── main.py                        # Pipeline entry point
```

---

## 🥉 Bronze Layer — Raw Ingestion

Three tables loaded as-is from source files. No transformations. Data fidelity is the priority.

| Table | Source | Rows |
|---|---|---|
| `bronze_wfp_food_prices` | WFP CSV | 121,512 |
| `bronze_psa_cpi` | PSA Excel (3 files) | 179,289 |
| `bronze_bsp_exchange_rate` | BSP live API | 1 (daily) |

---

## 🥈 Silver Layer — Cleaned & Standardized

dbt models clean and standardize each Bronze table:

- Region names standardized across all sources (e.g. "National Capital region" → "National Capital Region")
- PSA CPI unpivoted from wide format (years as columns) to long format (one row per month)
- Dates cast from VARCHAR to DATE
- Numeric values cast from VARCHAR to FLOAT/INT
- Duplicates removed via ROW_NUMBER()
- NULL values filtered

---

## 🥇 Gold Layer — Star Schema

Analytics-ready star schema built from Silver tables.

```
fact_food_prices (121,512 rows)
    ├── dim_region     (18 rows  — 17 PH regions + national)
    ├── dim_commodity  (73 rows  — 74 food commodities)
    ├── dim_date       (285 rows — monthly grain, 2000–2023)
    └── dim_market     (108 rows — markets across all regions)
```

**Key enriched columns in fact table:**
- `price_php` — actual market price in Philippine Peso
- `price_usd` — USD equivalent using historical rate
- `price_usd_current_rate` — USD equivalent using today's live BSP rate
- `regional_food_cpi` — PSA CPI index for that region and month
- `price_to_cpi_ratio` — inflation gap metric (market price ÷ CPI × 100)

---

## ✅ Data Quality Tests

20 dbt tests, all passing:

```
Done. PASS=20 WARN=0 ERROR=0 SKIP=0 TOTAL=20
```

Tests cover: `not_null`, `unique` on all primary keys and critical columns across all Silver and Gold models.

---

## ⚙️ Pipeline Orchestration

Automated monthly pipeline using Apache Airflow deployed on Astronomer Cloud:

```
Schedule: 0 6 1 * *  (6:00 AM on the 1st of every month)

Tasks:
  ingest_bronze     → Python script fetches WFP, PSA, BSP data
       ↓
  run_silver_models → dbt cleans and standardizes
       ↓
  run_gold_models   → dbt builds star schema
       ↓
  run_dbt_tests     → 20 data quality checks
```

---

## 📈 Key Findings

- **NCR has the highest inflation gap** — market food prices in Metro Manila are rising 60+ points faster than the official PSA CPI index
- **Mindanao regions** (BARMM, Region XII) consistently show the lowest food prices but also the largest negative inflation gaps — suggesting official CPI may overstate inflation in these regions
- **Meat, Fish and Eggs** category showed the steepest price increase (2018–2023), surging from ~₱140 to ~₱250/kg nationally
- **Luzon** (₱130.55 avg) is 19.7% more expensive than Mindanao (₱109.06 avg) for the same food basket

---

## 🚀 How to Run

### Prerequisites
- Python 3.8+
- Snowflake account
- dbt-snowflake installed

### Setup

```bash
# Clone the repository
git clone https://github.com/rolanddelarosaph/ph-food-pipeline.git
cd ph-food-pipeline

# Install dependencies
pip install -r requirements.txt

# Configure Snowflake credentials in src/config.py

# Initialize Snowflake environment
# Run sql/initialize_food_pipeline.sql in your Snowflake worksheet

# Run the pipeline
python main.py

# Run dbt transformations
cd ph_food_pipeline
dbt run
dbt test
```

---

## 📊 Statistical Design Notes

*This section reflects the author's background in Applied Statistics (Rizal Technological University).*

The `price_to_cpi_ratio` metric is computed as `price_php / regional_food_cpi × 100`. A ratio above 100 indicates market prices are inflating faster than the official CPI basket — suggesting either CPI underweighting of specific commodities or localized supply-side shocks not captured in the national index.

The grain of the fact table is one row per commodity per market per month — preserving maximum analytical flexibility while supporting aggregation at any level (region, island group, commodity category, time period).

Future work includes time-series forecasting using Facebook Prophet to project regional food prices 12 months forward, and Bayesian regression to quantify the causal relationship between peso depreciation and food price volatility.

---

## 👤 Author

**Roland Dela Rosa**
Statistics Graduate · Junior Data Engineer
[GitHub](https://github.com/rolanddelarosaph) · [LinkedIn](https://linkedin.com/in/rolanddelarosaph)

---

*Data sources: WFP Open Data · PSA OpenSTAT · Bangko Sentral ng Pilipinas*
*Pipeline built: April 2026*
# Philippine Food Price Analytics Pipeline

**Python · Snowflake · dbt · Apache Airflow · Tableau Public**

[![Dashboard](https://img.shields.io/badge/Tableau-Live%20Dashboard-blue?logo=tableau)](https://public.tableau.com/app/profile/roland.dela.rosa/viz/PHFoodPriceDashboard/DASHBOARD)
[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://www.python.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-Transformations-FF694A?logo=dbt)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-E27B34?logo=apacheairflow)](https://airflow.apache.org/)

---

## Architecture

![Pipeline Architecture](docs/architecture.png)

---

##  Live Dashboard

[![Philippine Food Price Analytics Dashboard](docs/ph_food_price_dashboard.png)](https://public.tableau.com/app/profile/roland.dela.rosa/viz/PHFoodPriceDashboard/DASHBOARD)

> **[→ Open interactive dashboard on Tableau Public](https://public.tableau.com/app/profile/roland.dela.rosa/viz/PHFoodPriceDashboard/DASHBOARD)**
> All 17 Philippine regions · 74 food commodities · 2000–2023 · Monthly grain

---

## Table of Contents

- [Project Overview](#-project-overview)
- [Tech Stack](#-tech-stack)
- [Data Sources](#-data-sources)
- [Project Structure](#️-project-structure)
- [Bronze — Raw Ingestion](#-bronze-layer--raw-ingestion)
- [Silver — Cleaned & Standardized](#-silver-layer--cleaned--standardized)
- [Gold — Star Schema](#-gold-layer--star-schema)
- [Data Quality Tests](#-data-quality-tests)
- [Pipeline Orchestration](#️-pipeline-orchestration)
- [Key Findings](#-key-findings)
- [Analytical Notebooks](#-analytical-notebooks)
- [How to Run](#-how-to-run)

---

##  Project Overview

This is an end-to-end cloud data pipeline that ingests real Philippine food price data from three government and institutional sources, transforms it through a Medallion Architecture in Snowflake, and serves it to a live Tableau dashboard covering all 17 regions of the Philippines.

The pipeline is built on the Modern Data Stack — Python handles ingestion, dbt handles SQL transformations and testing, Apache Airflow on Astronomer Cloud handles monthly scheduling, and Snowflake serves as the cloud data warehouse. The entire flow from raw source data to analytics-ready Gold tables runs automatically every month without manual intervention.

On top of the pipeline, two Jupyter notebooks perform the deeper statistical work: exploratory analysis across all regions and commodities, BSP exchange rate lag correlation, inflation gap decomposition by region, and Prophet time series forecasting for rice prices — demonstrating how statistical methods extend what the pipeline surfaces.

**What this pipeline processes:**
- 121,512 rows of WFP market food prices across 17 Philippine regions
- 179,289 rows of PSA Consumer Price Index data (regional, monthly)
- 101+ rows of BSP monthly USD/PHP exchange rates — historical CSV from Jan 2018, with live API appending one new row each monthly run
- 20 dbt data quality tests — all passing

---

##  Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python 3.12 | Fetch, parse, and load from CSV, Excel, and live JSON API |
| Storage | Snowflake | Cloud data warehouse across all three medallion layers |
| Transformation | dbt (dbt-snowflake) | SQL modeling, testing, and documentation |
| Orchestration | Apache Airflow on Astronomer | Monthly scheduling and task dependency management |
| Visualization | Tableau Public | Live interactive dashboard |
| Analysis | pandas, Prophet, statsmodels | EDA, forecasting, inflation gap analysis |

---

##  Data Sources

| Source | Description | Format | Rows |
|---|---|---|---|
| [WFP via Kaggle](https://www.kaggle.com/datasets/usmanlovescode/philippines-food-prices-dataset) | Actual market food prices, 17 PH regions | CSV | 121,512 |
| [PSA OpenSTAT](https://openstat.psa.gov.ph/) | Official Consumer Price Index by region | Excel (3 files) | 179,289 |
| [BSP / fawazahmed0 Currency API](https://www.bsp.gov.ph/) | Monthly USD/PHP rate — 100 months historical (Jan 2018–Apr 2026) + live API appends current month each run | CSV + JSON API | 101+ (compounding) |

---

##  Project Structure

```
ph-food-price-pipeline/
│
├── dags/
│   └── ph_food_pipeline_dag.py         # Airflow DAG — monthly 4-task pipeline
│
├── data/
│   ├── raw/                            # Source data files
│   │   ├── wfp_food_prices_phl.csv
│   │   ├── psa_cpi_2018_2020.xlsx
│   │   ├── psa_cpi_2021_2023.xlsx
│   │   ├── psa_cpi_2024_2026.xlsx
│   │   └── bsp_monthly_rates.csv       # BSP historical rates — compounds each run
│   └── exports/
│       └── ph_food_price_gold.csv      # Gold layer export used by notebooks
│
├── docs/                               # Architecture diagram and dashboard assets
│   ├── architecture.png
│   ├── ph_food_price_dashboard.png
│   └── ph_food_price_dashboard.twbx
│
├── notebooks/                          # Analytical notebooks
│   ├── ph_food_price_part1_eda_bsp.ipynb
│   └── ph_food_price_part2_inflation_forecasting.ipynb
│
├── ph_food_pipeline/                   # dbt project
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   │   ├── silver_food_prices.sql
│   │   │   ├── silver_psa_cpi.sql
│   │   │   └── silver_bsp_exchange_rate.sql
│   │   └── gold/
│   │       ├── dim_region.sql
│   │       ├── dim_commodity.sql
│   │       ├── dim_date.sql
│   │       ├── dim_market.sql
│   │       └── fact_food_prices.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── seeds/
│   │   └── bsp_monthly_rates.csv
│   ├── sources.yml
│   ├── schema.yml
│   └── dbt_project.yml
│
├── src/
│   ├── config.py                       # Snowflake connection config (env vars)
│   ├── ingest.py                       # Ingestion functions (WFP, PSA, BSP)
│   └── categorical_insert.py           # Dimension insert helpers
│
├── sql/
│   └── initialize_food_pipeline.sql    # Snowflake setup script
│
├── main.py                             # Pipeline entry point
├── requirements.txt
└── Dockerfile                          # Astronomer deployment config
```

---

##  Bronze Layer — Raw Ingestion

Three tables loaded as-is from each source. No transformations. The Bronze layer preserves source data exactly as received.

| Table | Source | Rows |
|---|---|---|
| `bronze_wfp_food_prices` | WFP CSV | 121,512 |
| `bronze_psa_cpi` | PSA Excel (3 files) | 179,289 |
| `bronze_bsp_exchange_rate` | BSP historical CSV + live API | 101+ (compounding) |

---

##  Silver Layer — Cleaned & Standardized

dbt models clean and standardize each Bronze table before it reaches the analytical layer.

- Region names standardized across all three sources
- PSA CPI unpivoted from wide format to long format (one row per region per month)
- Dates cast from `VARCHAR` to `DATE`; numeric values cast to `FLOAT` / `INT`
- Duplicates removed using `ROW_NUMBER()` window functions
- `NULL` values filtered on critical columns

---

##  Gold Layer — Star Schema

Analytics-ready star schema built from the Silver tables, designed for direct Tableau consumption and ad-hoc SQL analysis.

```
fact_food_prices (121,512 rows)
    ├── dim_region      (18 rows  — 17 PH regions + national)
    ├── dim_commodity   (73 rows  — 74 food commodities)
    ├── dim_date        (285 rows — monthly, Jan 2000 – Nov 2023)
    └── dim_market      (108 rows — local markets across all regions)
```

Key fact table columns include `price_php`, `price_usd` (historical BSP rate), `price_usd_current_rate` (live BSP rate), `regional_food_cpi`, and `price_to_cpi_ratio` — the core metric measuring how far market prices diverge from the official CPI index.

---

##  Data Quality Tests

20 dbt tests enforced across all Silver and Gold models — all passing:

```
Done. PASS=20  WARN=0  ERROR=0  SKIP=0  TOTAL=20
```

Tests cover `not_null` and `unique` constraints on all primary keys and critical columns. If any test fails, the pipeline halts before bad data reaches the Gold layer.

---

## ⚙️ Pipeline Orchestration

Monthly automated pipeline on **Apache Airflow** deployed via **Astronomer Cloud**:

```
Schedule: 0 6 1 * *   →   6:00 AM on the 1st of every month

  [ingest_bronze]       Python fetches WFP, PSA, and BSP data into Snowflake
        ↓
  [run_silver_models]   dbt cleans and standardizes all three sources
        ↓
  [run_gold_models]     dbt builds the star schema
        ↓
  [run_dbt_tests]       20 data quality checks — pipeline stops here on failure
```

---

##  Key Findings

> Sourced from the Gold layer, Tableau dashboard, and analytical notebooks.

- **NCR has the largest inflation gap** — market food prices in Metro Manila consistently run above what the official PSA CPI index implies, with the gap widening post-2020 and not fully recovering. This matters because social protection programs calibrated to official inflation may be systematically underestimating the pressure urban households face
- **BARMM and Northern Mindanao show negative gaps** — actual market prices in these major agricultural production zones run below the CPI index, a supply advantage that is invisible in national-level statistics
- **Luzon is 19.7% more expensive than Mindanao** on average (₱130.55 vs ₱109.06) for the same WFP food basket — a persistent regional disparity across the full dataset
- **Meat, Fish, and Eggs surged +80.6%** from 2018 to 2023, rising from ₱144/kg to ₱260/kg nationally — the steepest increase of any commodity category
- **Peso depreciation leads food prices by 1–2 months** — BSP lag correlation shows significant results at lag 1 (r = 0.29, p = 0.018) and lag 2 (r = 0.25, p = 0.042), confirming exchange rate as a leading indicator for import-linked food costs

---

##  Analytical Notebooks

The two notebooks in `notebooks/` perform statistical analysis on the Gold layer export from Snowflake. This is where the pipeline output becomes insight — the notebooks are the primary source of the findings above.

---

### Part 1 — EDA and BSP Exchange Rate Impact
`notebooks/ph_food_price_part1_eda_bsp.ipynb`

This notebook covers exploratory analysis across all 17 regions and 72 commodities, followed by a full BSP lag correlation study on 105,183 rows filtered to the 2018–2023 overlap period where all three data sources are complete.

**Exploratory findings:**
- National Capital Region is the most expensive region at ₱161.16/kg average; Bangsamoro Autonomous Region is the least expensive at ₱96.59/kg — a ₱64.56 gap across the same food basket
- Meat, Fish and Eggs is the most volatile and fastest-rising category, up 80.6% over 5 years
- The peso depreciated 22.6% from 2018 to 2023 (₱47.96 → ₱58.82 per USD)

**BSP lag correlation results:**
- Contemporaneous (same month): r = 0.32, p = 0.007 — significant
- Lag 1 month: r = 0.29, p = 0.018 — significant
- Lag 2 months: r = 0.25, p = 0.042 — significant
- Lag 3 months: r = 0.20, p = 0.101 — not significant

Exchange rate pressure takes 1–2 months to show up in market prices. Categories with higher import content are more sensitive to this effect than locally produced staples. A linear regression model using the 1-month lag as a predictor was also fitted to quantify the relationship.

---

### Part 2 — Inflation Gap Analysis and Rice Price Forecasting
`notebooks/ph_food_price_part2_inflation_forecasting.ipynb`

This notebook focuses on two analyses: regional inflation gap decomposition using the `price_to_cpi_ratio` from the Gold layer, and Prophet time series forecasting for rice prices with and without the BSP exchange rate as an external regressor.

**Inflation gap analysis:**
- The `price_to_cpi_ratio` (market price ÷ regional CPI × 100) is computed at the row level in the Gold layer and analyzed here by region, year, and commodity category
- A region × year heatmap surfaces which regions diverged most sharply from official CPI and when
- The post-2020 gap widening in NCR and Central Luzon is the most significant pattern in the dataset

**Rice price forecasting with Prophet:**
- Two models fitted: baseline (trend + seasonality only) and BSP-enhanced (with monthly exchange rate as external regressor)
- The BSP-enhanced model diverges from baseline during depreciation periods, confirming the exchange rate adds forecasting signal specifically under currency stress — the periods when food affordability pressure is most acute
- Seasonal decomposition confirms reliable Q3/Q4 price peaks tied to typhoon season
- Island-group forecasts show Luzon projecting the steepest continued increase; Mindanao starting from a lower base with more modest growth — national-average forecasts mask very different regional realities

---

##  How to Run

### Prerequisites
- Python 3.8+
- Snowflake account
- dbt-snowflake installed
- Astronomer account (for Airflow deployment) — or run Airflow locally

### Setup

```bash
# Clone the repository
git clone https://github.com/rolanddelarosaph/ph-food-price-pipeline.git
cd ph-food-price-pipeline

# Install dependencies
pip install -r requirements.txt

# Set up environment variables — create a .env file in the project root:
# SNOWFLAKE_ACCOUNT=your_account
# SNOWFLAKE_USER=your_user
# SNOWFLAKE_PASSWORD=your_password
# SNOWFLAKE_WAREHOUSE=your_warehouse
# SNOWFLAKE_DATABASE=ph_food_pipeline
# SNOWFLAKE_ROLE=your_role

# Initialize Snowflake environment
# Run sql/initialize_food_pipeline.sql in your Snowflake worksheet

# Run the ingestion pipeline
python main.py

# Run dbt transformations
cd ph_food_pipeline
dbt deps
dbt run
dbt test
```

### Notebooks

```bash
jupyter notebook notebooks/
```

Run Part 1 first, then Part 2. Both notebooks load from `data/exports/ph_food_price_gold.csv` — the Gold layer export from Snowflake.

---

*Data sources: [WFP via Kaggle](https://www.kaggle.com/datasets/usmanlovescode/philippines-food-prices-dataset) · [PSA OpenSTAT](https://openstat.psa.gov.ph/) · [Bangko Sentral ng Pilipinas](https://www.bsp.gov.ph/) · Built: May 2026*

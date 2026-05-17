# ============================================
# config.py
# Central configuration for the pipeline
# ============================================
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Project root
ROOT_DIR = Path(__file__).resolve().parent.parent

# Data paths
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"

# Snowflake connection
SNOWFLAKE_CONFIG = {
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user":      os.environ.get("SNOWFLAKE_USER"),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    "database":  os.environ.get("SNOWFLAKE_DATABASE"),
    "role":      os.environ.get("SNOWFLAKE_ROLE"),
}

# Source file config
WFP_FILE     = RAW_DATA_DIR / "wfp_food_prices_phl.csv"
PSA_CPI_GLOB = "psa_cpi_*.xlsx"

# BSP API
BSP_API_URL = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
BSP_MONTHLY_FILE = RAW_DATA_DIR / "bsp_monthly_rates.csv"

# ============================================
# ingest.py
# Loads all raw data sources into Snowflake
# Bronze layer tables
# ============================================

import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from src.config import (
    SNOWFLAKE_CONFIG,
    RAW_DATA_DIR,
    WFP_FILE,
    PSA_CPI_GLOB,
    BSP_API_URL,
)

logger = logging.getLogger(__name__)


# ── Snowflake connection ───────────────────────────────────────────────────────
def get_snowflake_conn():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    logger.info("Snowflake connection established.")
    return conn


# ── Generic loader ─────────────────────────────────────────────────────────────
def load_dataframe_to_snowflake(df: pd.DataFrame, table: str, conn) -> None:
    df.columns = [
        c.upper().replace(" ", "_").replace("-", "_").replace(".", "_")
        for c in df.columns
    ]
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table.upper(),
        database="PH_FOOD_PIPELINE",
        schema="BRONZE",
        auto_create_table=True,
        overwrite=True,
    )
    logger.info(f"  → Loaded {nrows} rows into BRONZE.{table.upper()}")


# ── WFP Food Prices ────────────────────────────────────────────────────────────
def ingest_wfp(conn) -> None:
    logger.info("[WFP] Loading food prices...")
    df = pd.read_csv(WFP_FILE, skiprows=1)

    # WFP columns have # prefix e.g. '#date' — strip them
    df.columns = [c.lstrip("#") for c in df.columns]

    # Remove metadata rows
    df = df[~df["date"].astype(str).str.startswith("#")]
    df = df.reset_index(drop=True)

    logger.info(f"  → WFP rows: {len(df)}")
    load_dataframe_to_snowflake(df, "bronze_wfp_food_prices", conn)


# ── PSA CPI ────────────────────────────────────────────────────────────────────
def ingest_psa_cpi(conn) -> None:
    logger.info("[PSA CPI] Loading CPI files...")
    psa_files = sorted(RAW_DATA_DIR.glob(PSA_CPI_GLOB))

    if not psa_files:
        raise FileNotFoundError(
            f"No PSA CPI files found matching {PSA_CPI_GLOB} in {RAW_DATA_DIR}"
        )

    all_records = []

    for filepath in psa_files:
        logger.info(f"  → Parsing {filepath.name}")
        wb = __import__('openpyxl').load_workbook(filepath)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        # ── Detect year/month header rows ─────────────────────────────────
        # Row 2 (index 2) has year values spread across columns
        # Row 3 (index 3) has month labels
        year_row  = rows[2]
        month_row = rows[3]

        # Build column index → (year, month) mapping
        col_map = {}
        current_year = None
        for col_idx, val in enumerate(year_row):
            if val is not None and str(val).strip().isdigit():
                current_year = int(str(val).strip())
            if current_year and col_idx < len(month_row):
                month_val = month_row[col_idx]
                if month_val and str(month_val).strip() not in ['', 'None', 'Ave']:
                    col_map[col_idx] = (current_year, str(month_val).strip())

        # ── Parse data rows ───────────────────────────────────────────────
        current_region = None
        for row in rows[4:]:  # data starts at row index 4
            if row[0] is not None and str(row[0]).strip() not in ['', 'None']:
                current_region = str(row[0]).strip()

            commodity = row[1]
            if commodity is None or str(commodity).strip() in ['', 'None']:
                continue
            commodity = str(commodity).strip()

            # Skip header-like rows
            if 'Consumer Price Index' in commodity or commodity == 'Jan':
                continue

            for col_idx, (year, month) in col_map.items():
                val = row[col_idx] if col_idx < len(row) else None
                if val is not None and str(val) not in ['', 'None', '..']:
                    try:
                        cpi_value = float(val)
                        all_records.append({
                            'region':       current_region,
                            'commodity':    commodity,
                            'year':         year,
                            'month':        month,
                            'cpi_value':    cpi_value,
                            'source_file':  filepath.name,
                        })
                    except (ValueError, TypeError):
                        continue

    df = pd.DataFrame(all_records)
    logger.info(f"  → PSA CPI total records parsed: {len(df)}")
    load_dataframe_to_snowflake(df, "bronze_psa_cpi", conn)

# ── BSP Exchange Rate ──────────────────────────────────────────────────────────
def ingest_bsp(conn) -> None:
    logger.info("[BSP] Fetching live USD/PHP exchange rate...")
    try:
        import urllib.request
        import json
        from datetime import date

        with urllib.request.urlopen(BSP_API_URL, timeout=15) as response:
            data = json.loads(response.read().decode())

        php_rate = data["usd"]["php"]
        fetch_date = date.today().isoformat()

        records = [{
            "fetch_date":    fetch_date,
            "base_currency": "USD",
            "target_currency": "PHP",
            "usd_php_rate":  php_rate,
            "source":        "currency-api.pages.dev",
        }]

        df = pd.DataFrame(records)
        logger.info(f"  → USD/PHP rate today: {php_rate}")
        load_dataframe_to_snowflake(df, "bronze_bsp_exchange_rate", conn)

    except Exception as e:
        logger.warning(f"  Exchange rate API failed: {e}")
        logger.warning("  Skipping BSP ingestion this run.")


# ── Main entrypoint ────────────────────────────────────────────────────────────
def run_ingestion() -> None:
    conn = get_snowflake_conn()
    try:
        ingest_wfp(conn)
        ingest_psa_cpi(conn)
        ingest_bsp(conn)
        logger.info("All bronze tables loaded successfully.")
    finally:
        conn.close()

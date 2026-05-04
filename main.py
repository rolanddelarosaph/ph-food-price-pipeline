# ============================================
# main.py
# Pipeline entry point
# ============================================

import sys
import time
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingest import run_ingestion

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "pipeline.log", encoding="utf-8"),
    ],
)

if __name__ == "__main__":
    start = time.perf_counter()
    logging.info("=" * 55)
    logging.info("  PIPELINE STARTED")
    logging.info("=" * 55)
    try:
        run_ingestion()
        elapsed = time.perf_counter() - start
        logging.info(f"Total elapsed time: {elapsed:.2f}s")
        sys.exit(0)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
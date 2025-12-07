from pathlib import Path

# raíz del proyecto (etl_crm_project)
ROOT_DIR = Path(__file__).resolve().parents[2]

CONFIG_DIR = ROOT_DIR / "config"
DATALAKE_DIR = ROOT_DIR / "datalake"

DROP_ZONE_DIR = DATALAKE_DIR / "drop_zone"
RAW_DIR = DATALAKE_DIR / "raw"
PROCESSED_DIR = DATALAKE_DIR / "processed"
CURATED_DIR = DATALAKE_DIR / "curated"

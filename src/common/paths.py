# src/common/paths.py
from pathlib import Path

def project_root() -> Path:
    # .../etl_crm_project/src/common/paths.py -> subir 2 niveles
    return Path(__file__).resolve().parents[2]

def raw_dir(source: str, dataset: str) -> Path:
    return project_root() / "datalake" / "raw" / source / dataset

def processed_dir(source: str, dataset: str) -> Path:
    return project_root() / "datalake" / "processed" / source / dataset

def rejected_dir(source: str, dataset: str) -> Path:
    return project_root() / "datalake" / "rejected" / source / dataset

def schemas_dir(source: str) -> Path:
    return project_root() / "config" / "schemas" / source

def processed_source_dir(source: str) -> Path:
    return project_root() / "datalake" / "processed" / source

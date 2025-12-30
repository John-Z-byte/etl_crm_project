# src/common/paths.py
from pathlib import Path

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]

# ---------- RAW ----------
def raw_source_dir(source: str) -> Path:
    return project_root() / "datalake" / "raw" / source

def raw_dataset_dir(source: str, dataset: str) -> Path:
    return raw_source_dir(source) / dataset


# ---------- PROCESSED ----------
def processed_source_dir(source: str) -> Path:
    return project_root() / "datalake" / "processed" / source

def processed_dataset_dir(source: str, dataset: str) -> Path:
    return processed_source_dir(source) / dataset


# ---------- REJECTED ----------
def rejected_source_dir(source: str) -> Path:
    return project_root() / "datalake" / "rejected" / source

def rejected_dataset_dir(source: str, dataset: str) -> Path:
    return rejected_source_dir(source) / dataset


# ---------- SCHEMAS ----------
def schemas_dir(source: str) -> Path:
    return project_root() / "config" / "schemas" / source

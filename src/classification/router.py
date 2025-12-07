from __future__ import annotations

from datetime import date
from pathlib import Path

from .schemas_loader import Schema


def build_raw_target_path(
    datalake_root: Path,
    schema: Schema,
    load_date: date,
    original_filename: str,
) -> Path:
    """
    Build the final destination path for a matched file in the RAW layer.

    Pattern:
        {datalake_root}/raw/{source_system}/{dataset_name}/load_date=YYYY-MM-DD/{original_filename}
    """
    load_date_str = load_date.strftime("%Y-%m-%d")

    return (
        datalake_root
        / "raw"
        / schema.source_system
        / schema.dataset_name
        / f"load_date={load_date_str}"
        / original_filename
    )


def build_unclassified_path(datalake_root: Path, original_filename: str) -> Path:
    """
    Build destination path for files that could not be matched to any schema.
    """
    return (
        datalake_root
        / "drop_zone"
        / "unclassified"
        / original_filename
    )


def build_rejected_path(datalake_root: Path, original_filename: str) -> Path:
    """
    Build destination path for files that failed to be read or parsed.
    """
    return (
        datalake_root
        / "drop_zone"
        / "rejected"
        / original_filename
    )

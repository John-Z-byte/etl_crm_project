from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import List
import shutil

from src.classification.schemas_loader import load_all_schemas
from src.classification.file_inspector import inspect_file, FileProfile
from src.classification.matcher import match_file_to_schema, MatchResult
from src.classification.file_mover import (
    move_file_according_to_result,
    write_classification_log,
    ClassificationRecord,
)
from src.classification.router import build_rejected_path


def _reject_on_error(
    path: Path,
    datalake_root: Path,
    reason: str,
) -> ClassificationRecord:
    """
    Fallback when something goes wrong before normal classification.
    Sends the file to drop_zone/rejected.
    """
    target = build_rejected_path(datalake_root, path.name)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), str(target))

    return ClassificationRecord(
        original_path=path,
        target_path=target,
        status="rejected",
        schema_id=None,
        source_system=None,
        dataset_name=None,
        reason=reason,
        header_row_index=None,
        timestamp=datetime.utcnow(),
    )


def run_classify_drop_zone(
    datalake_root: Path | None = None,
    max_rows: int = 50,
) -> None:
    """
    Main entry point for classifying all files in drop_zone/incoming.

    Steps:
    - load all schemas
    - iterate over incoming files
    - inspect -> match -> move -> log
    """
    if datalake_root is None:
        datalake_root = Path("datalake")

    incoming_dir = datalake_root / "drop_zone" / "incoming"
    schemas_dir = Path("config") / "schemas"

    if not incoming_dir.is_dir():
        print(f"[WARN] Incoming directory does not exist: {incoming_dir}")
        return

    schemas = load_all_schemas(schemas_dir)
    load_date = date.today()

    records: List[ClassificationRecord] = []

    for path in incoming_dir.iterdir():
        if not path.is_file():
            continue

        try:
            file_profile: FileProfile = inspect_file(path, max_rows=max_rows)
            match: MatchResult = match_file_to_schema(file_profile, schemas)
            record = move_file_according_to_result(
                file_profile=file_profile,
                match=match,
                datalake_root=datalake_root,
                load_date=load_date,
            )
        except Exception as exc:
            # Any unexpected error -> reject file
            record = _reject_on_error(
                path=path,
                datalake_root=datalake_root,
                reason=str(exc),
            )

        records.append(record)

    # Write log for the whole batch
    if records:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        log_path = (
            datalake_root
            / "drop_zone"
            / "classification_logs"
            / f"classification_{ts}.csv"
        )
        write_classification_log(records, log_path)
        print(f"[INFO] Classification log written to: {log_path}")

        matched = sum(1 for r in records if r.status == "matched")
        unclassified = sum(1 for r in records if r.status == "unclassified")
        rejected = sum(1 for r in records if r.status == "rejected")

        print(
            f"[SUMMARY] matched={matched}, "
            f"unclassified={unclassified}, rejected={rejected}, "
            f"total={len(records)}"
        )
    else:
        print("[INFO] No files found in incoming.")

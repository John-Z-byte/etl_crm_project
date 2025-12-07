from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List
import shutil
import csv

from .file_inspector import FileProfile
from .matcher import MatchResult
from .router import (
    build_raw_target_path,
    build_unclassified_path,
    build_rejected_path,
)


@dataclass
class ClassificationRecord:
    """
    Single record describing the result of classifying and moving one file.
    """
    original_path: Path
    target_path: Optional[Path]
    status: str                      # "matched" | "unclassified" | "rejected"
    schema_id: Optional[str]
    source_system: Optional[str]
    dataset_name: Optional[str]
    reason: Optional[str]
    header_row_index: Optional[int]
    timestamp: datetime


def move_file_according_to_result(
    file_profile: FileProfile,
    match: MatchResult,
    datalake_root: Path,
    load_date: date,
) -> ClassificationRecord:
    """
    Move a file to the correct location according to the match result.

    - matched      -> RAW:  raw/{source}/{dataset}/load_date=YYYY-MM-DD/{filename}
    - unclassified -> drop_zone/unclassified/{filename}
    - rejected     -> drop_zone/rejected/{filename}
    """
    original_path = file_profile.path
    filename = original_path.name

    target_path: Optional[Path] = None
    schema_id: Optional[str] = None
    source_system: Optional[str] = None
    dataset_name: Optional[str] = None

    # Decide destination path
    if match.status == "matched" and match.schema is not None:
        schema_id = match.schema.id
        source_system = match.schema.source_system
        dataset_name = match.schema.dataset_name

        target_path = build_raw_target_path(
            datalake_root=datalake_root,
            schema=match.schema,
            load_date=load_date,
            original_filename=filename,
        )
    elif match.status == "unclassified":
        target_path = build_unclassified_path(
            datalake_root=datalake_root,
            original_filename=filename,
        )
    elif match.status == "rejected":
        target_path = build_rejected_path(
            datalake_root=datalake_root,
            original_filename=filename,
        )
    else:
        # Defensive fallback: treat unknown status as unclassified
        target_path = build_unclassified_path(
            datalake_root=datalake_root,
            original_filename=filename,
        )

    # Ensure target directory exists
    if target_path is not None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        # Move the file
        shutil.move(str(original_path), str(target_path))

    record = ClassificationRecord(
        original_path=original_path,
        target_path=target_path,
        status=match.status,
        schema_id=schema_id,
        source_system=source_system,
        dataset_name=dataset_name,
        reason=match.reason,
        header_row_index=getattr(match, "header_row_index", None),
        timestamp=datetime.utcnow(),
    )
    return record


def write_classification_log(
    records: List[ClassificationRecord],
    log_path: Path,
) -> None:
    """
    Persist a simple CSV log with the classification results for a batch run.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_utc",
                "original_path",
                "target_path",
                "status",
                "schema_id",
                "source_system",
                "dataset_name",
                "reason",
                "header_row_index",
            ]
        )

        for r in records:
            writer.writerow(
                [
                    r.timestamp.isoformat(),
                    str(r.original_path),
                    str(r.target_path) if r.target_path is not None else "",
                    r.status,
                    r.schema_id or "",
                    r.source_system or "",
                    r.dataset_name or "",
                    r.reason or "",
                    r.header_row_index if r.header_row_index is not None else "",
                ]
            )

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
import fnmatch

from .schemas_loader import Schema
from .file_inspector import FileProfile


@dataclass
class MatchResult:
    """
    Result of trying to match a file to a schema.
    """
    status: str                     # "matched" | "unclassified" | "rejected"
    schema: Optional[Schema]
    reason: Optional[str] = None
    header_row_index: Optional[int] = None
    matched_columns: List[str] = None


def _matches_any_pattern(filename: str, patterns: List[str]) -> bool:
    """
    Check if a filename matches at least one of the given glob-like patterns.
    """
    return any(fnmatch.fnmatch(filename, pattern) for pattern in patterns)


def _find_header_row_for_schema(
    schema: Schema,
    rows: List[List[str]],
) -> Tuple[bool, Optional[int], List[str]]:
    """
    Given a schema and the list of rows from a file, try to find a row
    that can be considered the header for this schema.

    A row is considered a valid header if it contains ALL required_columns
    from the schema (as exact string matches).

    Returns:
        (is_match, header_row_index, matched_columns)
    """
    required = set(schema.required_columns)

    for idx, row in enumerate(rows):
        # Normalize row values as a set of non-empty strings
        row_values = {cell for cell in row if cell}

        # If the row doesn't even have the right size, skip quickly
        if not row_values:
            continue

        if required.issubset(row_values):
            # All required columns are present in this row
            return True, idx, list(required)

    return False, None, []


def match_file_to_schema(
    file_profile: FileProfile,
    schemas: List[Schema],
) -> MatchResult:
    """
    Try to match a file (FileProfile) to one of the available schemas.

    Steps:
    1. Filter schemas by file_patterns using the filename.
    2. For each candidate, try to find a header row that contains all required_columns.
    3. Choose the "best" schema:
       - any schema that fully matches all required_columns
       - if multiple match, pick the one with more required_columns
         (more specific) and, as tie-breaker, the earliest header_row_index.

    If no schema matches, returns status="unclassified".
    """
    filename = file_profile.path.name

    # 1) Filter by file_patterns (coarse filtering by filename)
    candidate_schemas: List[Schema] = [
        s for s in schemas if _matches_any_pattern(filename, s.file_patterns)
    ]

    if not candidate_schemas:
        return MatchResult(
            status="unclassified",
            schema=None,
            reason="no_file_pattern_match",
            header_row_index=None,
            matched_columns=[],
        )

    # 2) Evaluate header match for each candidate
    best_schema: Optional[Schema] = None
    best_header_row_index: Optional[int] = None
    best_required_count: int = -1

    for schema in candidate_schemas:
        is_match, header_idx, matched_cols = _find_header_row_for_schema(
            schema, file_profile.rows
        )

        if not is_match or header_idx is None:
            continue

        required_count = len(schema.required_columns)

        # Select schema with:
        # - more required columns (more specific)
        # - if tie, earlier header row index
        if (
            required_count > best_required_count
            or (
                required_count == best_required_count
                and (best_header_row_index is None or header_idx < best_header_row_index)
            )
        ):
            best_schema = schema
            best_header_row_index = header_idx
            best_required_count = required_count

    if best_schema is None:
        return MatchResult(
            status="unclassified",
            schema=None,
            reason="no_header_match_for_any_candidate",
            header_row_index=None,
            matched_columns=[],
        )

    # 3) Success
    return MatchResult(
        status="matched",
        schema=best_schema,
        reason=None,
        header_row_index=best_header_row_index,
        matched_columns=list(best_schema.required_columns),
    )

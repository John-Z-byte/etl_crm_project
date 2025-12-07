from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List
import pandas as pd

from .header_detector import read_rows
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

@dataclass
class FileProfile:
    """
    Lightweight description of a file found in the drop zone.

    It contains:
    - the original path
    - the file extension
    - the first N rows as plain strings (to be inspected by the matcher)
    """
    path: Path
    extension: str
    rows: List[List[str]]


def inspect_file(path: Path, max_rows: int = 50) -> FileProfile:
    """
    Inspect a file and return a FileProfile with basic information.

    This function does NOT decide which row is the header.
    It only reads a limited number of rows and leaves the decision
    to the matching logic, which will compare rows against schema.required_columns.
    """
    if not path.is_file():
        raise FileNotFoundError(f"File does not exist: {path}")

    ext = path.suffix.lower()
    rows = read_rows(path, max_rows=max_rows)

    return FileProfile(
        path=path,
        extension=ext,
        rows=rows,
    )

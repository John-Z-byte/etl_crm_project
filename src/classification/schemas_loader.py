from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List

import yaml


@dataclass
class Schema:
    """In-memory representation of a dataset schema definition."""
    id: str
    source_system: str
    dataset_name: str
    version: str = "1.0"

    description: Optional[str] = None

    file_patterns: List[str] = field(default_factory=list)

    # Solo aplican a CSV; para Excel quedarán en None
    expected_delimiter: Optional[str] = None
    expected_encoding: Optional[str] = None

    allow_extra_columns: bool = True

    required_columns: List[str] = field(default_factory=list)
    optional_columns: List[str] = field(default_factory=list)


def load_schema(path: Path) -> Schema:
    """
    Load a single YAML schema file into a Schema object.
    Raises ValueError if required keys are missing.
    """
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    # Validaciones mínimas
    missing = [
        key
        for key in ("id", "source_system", "dataset_name", "file_patterns", "required_columns")
        if key not in data or data[key] in (None, [], "")
    ]
    if missing:
        raise ValueError(f"Schema file {path} is missing required keys: {', '.join(missing)}")

    return Schema(
        id=data["id"],
        source_system=data["source_system"],
        dataset_name=data["dataset_name"],
        version=str(data.get("version", "1.0")),
        description=data.get("description"),
        file_patterns=list(data.get("file_patterns", [])),
        expected_delimiter=data.get("expected_delimiter"),
        expected_encoding=data.get("expected_encoding"),
        allow_extra_columns=bool(data.get("allow_extra_columns", True)),
        required_columns=list(data.get("required_columns", [])),
        optional_columns=list(data.get("optional_columns", [])),
    )


def load_all_schemas(config_dir: Path) -> List[Schema]:
    """
    Load all *.yaml / *.yml schema files from a directory.

    Example:
        schemas = load_all_schemas(Path("config/schemas"))
    """
    if not config_dir.is_dir():
        raise ValueError(f"Schema directory does not exist: {config_dir}")

    schema_files = sorted(
        list(config_dir.glob("*.yaml")) + list(config_dir.glob("*.yml"))
    )

    schemas: List[Schema] = []
    for path in schema_files:
        schema = load_schema(path)
        schemas.append(schema)

    return schemas

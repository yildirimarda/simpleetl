"""
Schema registry for SimpleETL.

Provides abstract and file-based schema registries for storing and
retrieving versioned schemas.
"""

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List

from simpleetl.core.schema import Schema

logger = logging.getLogger(__name__)


class SchemaRegistry(ABC):
    """Abstract base class for schema registries.

    A schema registry stores versioned schemas and provides methods
    to register, retrieve, and list them.
    """

    @abstractmethod
    def register_schema(
        self, name: str, version: int, schema: Schema
    ) -> None:
        """Register a schema under the given name and version.

        Args:
            name: Schema name (e.g. ``"users"``).
            version: Positive integer version number.
            schema: Schema instance to register.
        """
        ...

    @abstractmethod
    def get_schema(self, name: str, version: int) -> Schema:
        """Retrieve a specific version of a schema.

        Args:
            name: Schema name.
            version: Version number.

        Returns:
            The requested Schema.

        Raises:
            KeyError: If the schema name or version is not found.
        """
        ...

    @abstractmethod
    def get_latest_schema(self, name: str) -> Schema:
        """Retrieve the latest version of a schema.

        Args:
            name: Schema name.

        Returns:
            The latest Schema.

        Raises:
            KeyError: If the schema name is not found.
        """
        ...

    @abstractmethod
    def list_versions(self, name: str) -> List[int]:
        """List all registered versions for a schema name.

        Args:
            name: Schema name.

        Returns:
            Sorted list of version numbers.

        Raises:
            KeyError: If the schema name is not found.
        """
        ...

    @abstractmethod
    def list_schemas(self) -> List[str]:
        """List all registered schema names.

        Returns:
            Sorted list of schema names.
        """
        ...


class FileSchemaRegistry(SchemaRegistry):
    """JSON file-based schema registry.

    Schemas are stored as JSON files in a directory structure::

        base_dir/
        {name}/
            v{version}.json

    Args:
        base_dir: Root directory for schema storage.
    """

    def __init__(self, base_dir: str | Path):
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    # -- helpers ------------------------------------------------------------

    def _schema_dir(self, name: str) -> Path:
        return self._base_dir / name

    def _schema_path(self, name: str, version: int) -> Path:
        return self._schema_dir(name) / f"v{version}.json"

    def _load_json(self, path: Path) -> Dict[str, Any]:
        with open(path, "r") as f:
            return json.load(f)

    def _save_json(self, path: Path, data: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    # -- SchemaRegistry implementation --------------------------------------

    def register_schema(
        self, name: str, version: int, schema: Schema
    ) -> None:
        """Register a schema under the given name and version."""
        if version < 1:
            raise ValueError(
                f"Version must be a positive integer, got {version}"
            )
        path = self._schema_path(name, version)
        self._save_json(path, schema.to_dict())
        logger.info(
            "Registered schema '%s' version %d at %s", name, version, path
        )

    def get_schema(self, name: str, version: int) -> Schema:
        """Retrieve a specific version of a schema."""
        path = self._schema_path(name, version)
        if not path.exists():
            raise KeyError(
                f"Schema '{name}' version {version} not found at {path}"
            )
        data = self._load_json(path)
        return Schema.from_dict(data)

    def get_latest_schema(self, name: str) -> Schema:
        """Retrieve the latest version of a schema."""
        try:
            versions = self.list_versions(name)
        except KeyError:
            raise KeyError(f"No versions found for schema '{name}'")
        if not versions:
            raise KeyError(f"No versions found for schema '{name}'")
        latest = max(versions)
        return self.get_schema(name, latest)

    def list_versions(self, name: str) -> List[int]:
        """List all registered versions for a schema name."""
        schema_dir = self._schema_dir(name)
        if not schema_dir.exists():
            raise KeyError(f"Schema '{name}' not found")
        versions: List[int] = []
        for p in schema_dir.iterdir():
            if p.suffix == ".json" and p.stem.startswith("v"):
                try:
                    versions.append(int(p.stem[1:]))
                except ValueError:
                    continue
        return sorted(versions)

    def list_schemas(self) -> List[str]:
        """List all registered schema names."""
        names: List[str] = []
        for p in self._base_dir.iterdir():
            if p.is_dir():
                names.append(p.name)
        return sorted(names)

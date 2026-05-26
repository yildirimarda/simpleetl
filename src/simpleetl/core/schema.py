"""
Schema management for SimpleETL.

Provides classes for defining, validating, comparing, and evolving
data schemas, plus DDL generation for common SQL dialects.
"""

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd


# ---------------------------------------------------------------------------
# Dialect enum
# ---------------------------------------------------------------------------


class SQLDialect(str, Enum):
    """Supported SQL dialects for DDL generation."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"


# ---------------------------------------------------------------------------
# ColumnDef
# ---------------------------------------------------------------------------


@dataclass
class ColumnDef:
    """Definition of a single column in a schema.

    Attributes:
        name: Column name.
        dtype: Pandas-compatible dtype string (e.g. ``"int64"``,
            ``"float64"``, ``"object"``, ``"bool"``, ``"datetime64[ns]"``).
        nullable: Whether the column allows null values.
        default: Default value when the column is missing or null.
        description: Human-readable description of the column.
        struct_type: Optional StructType for nested struct columns.
        array_type: Optional ArrayType for array/list columns.
        map_type: Optional MapType for map/key-value columns.
    """

    name: str
    dtype: str
    nullable: bool = True
    default: Any = None
    description: str = ""
    struct_type: Optional["StructType"] = None
    array_type: Optional["ArrayType"] = None
    map_type: Optional["MapType"] = None

    @property
    def is_nested(self) -> bool:
        """Return True if this column has a nested/complex type."""
        return any([self.struct_type, self.array_type, self.map_type])

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        d: Dict[str, Any] = {
            "name": self.name,
            "dtype": self.dtype,
            "nullable": self.nullable,
            "default": self.default,
            "description": self.description,
        }
        if self.struct_type:
            d["struct_type"] = self.struct_type.to_dict()
        if self.array_type:
            d["array_type"] = self.array_type.to_dict()
        if self.map_type:
            d["map_type"] = self.map_type.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnDef":
        """Create a ColumnDef from a dictionary."""
        col = cls(
            name=data["name"],
            dtype=data["dtype"],
            nullable=data.get("nullable", True),
            default=data.get("default"),
            description=data.get("description", ""),
        )
        if "struct_type" in data:
            col.struct_type = StructType.from_dict(data["struct_type"])
        if "array_type" in data:
            col.array_type = ArrayType.from_dict(data["array_type"])
        if "map_type" in data:
            col.map_type = MapType.from_dict(data["map_type"])
        return col


# ---------------------------------------------------------------------------
# Nested / complex type definitions
# ---------------------------------------------------------------------------


class FieldDef:
    """A field within a nested struct type."""

    def __init__(self, name: str, dtype: str, nullable: bool = True) -> None:
        self.name = name
        self.dtype = dtype
        self.nullable = nullable

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {"name": self.name, "dtype": self.dtype, "nullable": self.nullable}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FieldDef":
        """Create a FieldDef from a dictionary."""
        return cls(
            name=data["name"],
            dtype=data["dtype"],
            nullable=data.get("nullable", True),
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FieldDef):
            return NotImplemented
        return (
            self.name == other.name
            and self.dtype == other.dtype
            and self.nullable == other.nullable
        )


class StructType:
    """Represents a nested struct (record) type."""

    def __init__(self, fields: List[FieldDef]) -> None:
        self.fields = fields

    @property
    def dtype(self) -> str:
        """Return the dtype string representation of this struct."""
        field_strs = [f"{f.name}:{f.dtype}" for f in self.fields]
        return f"struct<{','.join(field_strs)}>"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {"type": "struct", "fields": [f.to_dict() for f in self.fields]}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StructType":
        """Create a StructType from a dictionary."""
        return cls(fields=[FieldDef.from_dict(f) for f in data["fields"]])

    def merge(self, other: "StructType") -> "StructType":
        """Merge two struct types (additive -- new fields are added)."""
        existing = {f.name: f for f in self.fields}
        new_fields = list(self.fields)
        for f in other.fields:
            if f.name not in existing:
                new_fields.append(f)
        return StructType(new_fields)


class ArrayType:
    """Represents an array (list) type."""

    def __init__(self, element_type: str) -> None:
        self.element_type = element_type

    @property
    def dtype(self) -> str:
        """Return the dtype string representation of this array."""
        return f"array<{self.element_type}>"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {"type": "array", "element_type": self.element_type}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArrayType":
        """Create an ArrayType from a dictionary."""
        return cls(element_type=data["element_type"])


class MapType:
    """Represents a map (key-value) type."""

    def __init__(self, key_type: str, value_type: str) -> None:
        self.key_type = key_type
        self.value_type = value_type

    @property
    def dtype(self) -> str:
        """Return the dtype string representation of this map."""
        return f"map<{self.key_type},{self.value_type}>"

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "type": "map",
            "key_type": self.key_type,
            "value_type": self.value_type,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MapType":
        """Create a MapType from a dictionary."""
        return cls(key_type=data["key_type"], value_type=data["value_type"])


# ---------------------------------------------------------------------------
# SchemaDiff
# ---------------------------------------------------------------------------


@dataclass
class SchemaDiff:
    """Result of comparing two schemas.

    Attributes:
        added_columns: Columns present in the new schema but not in the old.
        removed_columns: Columns present in the old schema but not in the new.
        type_changes: Columns whose dtype changed. Maps column name to
            ``{"old": str, "new": str}``.
        nullability_changes: Columns whose nullability changed. Maps column
            name to ``{"old": bool, "new": bool}``.
    """

    added_columns: List[str] = field(default_factory=list)
    removed_columns: List[str] = field(default_factory=list)
    type_changes: Dict[str, Dict[str, str]] = field(default_factory=dict)
    nullability_changes: Dict[str, Dict[str, bool]] = field(
        default_factory=dict
    )

    @property
    def has_changes(self) -> bool:
        """Return True if any differences were found."""
        return bool(
            self.added_columns
            or self.removed_columns
            or self.type_changes
            or self.nullability_changes
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "added_columns": self.added_columns,
            "removed_columns": self.removed_columns,
            "type_changes": self.type_changes,
            "nullability_changes": self.nullability_changes,
        }


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class Schema:
    """Represents a data schema with columns, types, and metadata.

    A Schema is an ordered collection of :class:`ColumnDef` objects plus
    optional free-form metadata.

    Example::

        schema = Schema(
            columns=[
                ColumnDef("id", "int64", nullable=False),
                ColumnDef("name", "object"),
            ],
            metadata={"source": "users_table"},
        )
    """

    def __init__(
        self,
        columns: Sequence[ColumnDef],
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self._columns: List[ColumnDef] = list(columns)
        self.metadata: Dict[str, Any] = metadata or {}

    # -- properties ---------------------------------------------------------

    @property
    def columns(self) -> List[ColumnDef]:
        """Return the list of column definitions."""
        return list(self._columns)

    @property
    def column_names(self) -> List[str]:
        """Return the ordered list of column names."""
        return [c.name for c in self._columns]

    def get_column(self, name: str) -> Optional[ColumnDef]:
        """Return the ColumnDef for *name*, or ``None`` if not found."""
        for col in self._columns:
            if col.name == name:
                return col
        return None

    # -- factory methods ----------------------------------------------------

    @staticmethod
    def _infer_scalar_dtype(py_type: type) -> str:
        """Map a Python type to a simple dtype string."""
        mapping = {
            int: "int64",
            float: "float64",
            str: "string",
            bool: "bool",
        }
        return mapping.get(py_type, "string")

    @staticmethod
    def _infer_list_element_type(series: "pd.Series") -> str:
        """Infer the element type of list-typed column values."""
        for val in series:
            if isinstance(val, list) and len(val) > 0:
                return Schema._infer_scalar_dtype(type(val[0]))
        return "string"

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        nullable: Optional[Dict[str, bool]] = None,
        descriptions: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "Schema":
        """Infer a schema from a DataFrame.

        Args:
            df: Source DataFrame.
            nullable: Optional per-column override for nullability.
                Columns that contain no nulls are marked non-nullable
                by default; columns with any nulls are marked nullable.
            descriptions: Optional per-column description strings.
            metadata: Optional schema-level metadata.

        Returns:
            A new Schema instance.
        """
        nullable = nullable or {}
        descriptions = descriptions or {}
        cols: List[ColumnDef] = []

        for col_name in df.columns:
            col_series = df[col_name]
            dtype = str(col_series.dtype)
            is_nullable = nullable.get(
                col_name, bool(col_series.isna().any())
            )

            # Build a base ColumnDef first
            col_def = ColumnDef(
                name=str(col_name),
                dtype=dtype,
                nullable=is_nullable,
                description=descriptions.get(col_name, ""),
            )

            # Infer nested types from non-null values
            non_null = col_series.dropna()
            if len(non_null) > 0:
                first_val = non_null.iloc[0]

                if isinstance(first_val, dict):
                    # Infer StructType from dict
                    fields: List[FieldDef] = []
                    for k in first_val:
                        inferred = cls._infer_scalar_dtype(type(first_val[k]))
                        fields.append(FieldDef(name=str(k), dtype=inferred))
                    col_def.struct_type = StructType(fields=fields)
                    col_def.dtype = col_def.struct_type.dtype

                elif isinstance(first_val, list):
                    # Infer ArrayType from list
                    elem_dtype = cls._infer_list_element_type(non_null)
                    col_def.array_type = ArrayType(element_type=elem_dtype)
                    col_def.dtype = col_def.array_type.dtype

                elif isinstance(first_val, dict):
                    # MapType inference: dicts with consistent key types.
                    # This branch is unreachable here because the struct
                    # branch above already catches dicts; MapType inference
                    # is handled explicitly by the caller when needed.
                    pass

            cols.append(col_def)

        return cls(columns=cols, metadata=metadata)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Schema":
        """Create a Schema from a dictionary.

        Expected format::

            {
                "columns": [
                    {"name": "id", "dtype": "int64", "nullable": false},
                    ...
                ],
                "metadata": {"key": "value"},
            }

        Args:
            data: Dictionary representation of a schema.

        Returns:
            A new Schema instance.
        """
        columns = [ColumnDef.from_dict(c) for c in data.get("columns", [])]
        return cls(columns=columns, metadata=data.get("metadata", {}))

    # -- serialization ------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the schema to a dictionary."""
        return {
            "columns": [c.to_dict() for c in self._columns],
            "metadata": self.metadata,
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize the schema to a JSON string.

        Args:
            indent: JSON indentation level.

        Returns:
            JSON string representation.
        """
        return json.dumps(self.to_dict(), indent=indent, default=str)

    # -- validation ---------------------------------------------------------

    def validate(
        self,
        df: pd.DataFrame,
        strict_nullability: bool = False,
        strict_types: bool = False,
    ) -> List[str]:
        """Validate a DataFrame against this schema.

        Checks that all required columns are present, optionally verifies
        types and nullability constraints.

        Args:
            df: DataFrame to validate.
            strict_nullability: If True, raise on nullable columns that
                contain nulls.
            strict_types: If True, raise on dtype mismatches.

        Returns:
            An empty list if validation passes.

        Raises:
            SchemaValidationError: If validation fails.
        """
        errors: List[str] = []

        # Check for missing columns
        df_cols = set(df.columns)
        schema_cols = set(self.column_names)

        missing = schema_cols - df_cols
        if missing:
            errors.append(
                f"Missing columns: {sorted(missing)}"
            )

        # Check for extra columns (informational, not an error by default)
        extra = df_cols - schema_cols
        if extra:
            errors.append(
                f"Extra columns not in schema: {sorted(extra)}"
            )

        # Type and nullability checks on columns that exist in both
        for col_def in self._columns:
            if col_def.name not in df.columns:
                continue

            series = df[col_def.name]

            # Nullability check
            if strict_nullability and not col_def.nullable:
                null_count = int(series.isna().sum())
                if null_count > 0:
                    errors.append(
                        f"Column '{col_def.name}' is marked non-nullable "
                        f"but contains {null_count} null values"
                    )

            # Type check
            if strict_types:
                actual = str(series.dtype)
                if actual != col_def.dtype:
                    errors.append(
                        f"Column '{col_def.name}' expected type "
                        f"'{col_def.dtype}', got '{actual}'"
                    )

        if errors:
            raise SchemaValidationError(
                f"Schema validation failed with {len(errors)} error(s)",
                errors=errors,
            )

        return errors

    # -- diff / evolve / merge ---------------------------------------------

    def diff(self, other: "Schema") -> SchemaDiff:
        """Compare this schema to *other* and return the differences.

        Args:
            other: The schema to compare against.

        Returns:
            A SchemaDiff describing the changes.
        """
        self_cols = {c.name: c for c in self._columns}
        other_cols = {c.name: c for c in other._columns}

        added = [
            name for name in other_cols if name not in self_cols
        ]
        removed = [
            name for name in self_cols if name not in other_cols
        ]

        type_changes: Dict[str, Dict[str, str]] = {}
        nullability_changes: Dict[str, Dict[str, bool]] = {}

        for name in self_cols:
            if name not in other_cols:
                continue
            sc = self_cols[name]
            oc = other_cols[name]
            if sc.dtype != oc.dtype:
                type_changes[name] = {"old": sc.dtype, "new": oc.dtype}
            if sc.nullable != oc.nullable:
                nullability_changes[name] = {
                    "old": sc.nullable,
                    "new": oc.nullable,
                }

        return SchemaDiff(
            added_columns=sorted(added),
            removed_columns=sorted(removed),
            type_changes=type_changes,
            nullability_changes=nullability_changes,
        )

    def evolve(
        self,
        other: "Schema",
        allow_type_changes: bool = False,
        allow_nullability_changes: bool = True,
    ) -> "Schema":
        """Create an evolved schema based on *other*.

        The evolved schema starts as a copy of *self* and incorporates
        changes from *other*:

        - New columns are appended.
        - Removed columns are dropped.
        - Type changes are applied only when *allow_type_changes* is True.
        - Nullability changes are applied when *allow_nullability_changes*
          is True.

        Args:
            other: The target schema to evolve towards.
            allow_type_changes: Whether to allow dtype changes.
            allow_nullability_changes: Whether to allow nullability changes.

        Returns:
            A new evolved Schema.
        """
        diff = self.diff(other)
        other_map = {c.name: c for c in other._columns}

        evolved: List[ColumnDef] = []

        # Keep columns from self that are not removed, applying changes
        for col in self._columns:
            if col.name in diff.removed_columns:
                continue

            new_col = ColumnDef(
                name=col.name,
                dtype=col.dtype,
                nullable=col.nullable,
                default=col.default,
                description=col.description,
            )

            if col.name in diff.type_changes and allow_type_changes:
                new_col.dtype = diff.type_changes[col.name]["new"]

            if col.name in diff.nullability_changes and allow_nullability_changes:
                new_col.nullable = diff.nullability_changes[col.name]["new"]

            evolved.append(new_col)

        # Append added columns from other
        for name in diff.added_columns:
            evolved.append(
                ColumnDef(
                    name=other_map[name].name,
                    dtype=other_map[name].dtype,
                    nullable=other_map[name].nullable,
                    default=other_map[name].default,
                    description=other_map[name].description,
                )
            )

        merged_meta = dict(self.metadata)
        merged_meta.update(other.metadata)

        return Schema(columns=evolved, metadata=merged_meta)

    def merge(self, other: "Schema") -> "Schema":
        """Merge this schema with *other*, producing a union of columns.

        When both schemas define the same column, the definition from
        *self* takes precedence.

        Args:
            other: Schema to merge with.

        Returns:
            A new Schema containing the union of columns.
        """
        merged: Dict[str, ColumnDef] = {}
        for col in self._columns:
            merged[col.name] = col
        for col in other._columns:
            if col.name not in merged:
                merged[col.name] = col

        # Preserve order: self columns first, then new columns from other
        ordered: List[ColumnDef] = []
        seen = set()
        for col in self._columns:
            ordered.append(merged[col.name])
            seen.add(col.name)
        for col in other._columns:
            if col.name not in seen:
                ordered.append(merged[col.name])
                seen.add(col.name)

        merged_meta = dict(self.metadata)
        merged_meta.update(other.metadata)

        return Schema(columns=ordered, metadata=merged_meta)

    # -- dunder helpers -----------------------------------------------------

    def __repr__(self) -> str:
        cols = ", ".join(c.name for c in self._columns)
        return f"Schema(columns=[{cols}])"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Schema):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __len__(self) -> int:
        return len(self._columns)


# ---------------------------------------------------------------------------
# SchemaValidationError
# ---------------------------------------------------------------------------


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""

    def __init__(self, message: str, errors: Optional[List[str]] = None):
        self.errors = errors or []
        super().__init__(message)


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

# Mapping from pandas dtype strings to SQL type strings per dialect.
_DTYPE_MAP: Dict[str, Dict[str, str]] = {
    "int64": {"postgresql": "BIGINT", "mysql": "BIGINT", "sqlite": "INTEGER"},
    "int32": {"postgresql": "INTEGER", "mysql": "INT", "sqlite": "INTEGER"},
    "int16": {"postgresql": "SMALLINT", "mysql": "SMALLINT", "sqlite": "INTEGER"},
    "int8": {"postgresql": "SMALLINT", "mysql": "TINYINT", "sqlite": "INTEGER"},
    "float64": {"postgresql": "DOUBLE PRECISION", "mysql": "DOUBLE", "sqlite": "REAL"},
    "float32": {"postgresql": "REAL", "mysql": "FLOAT", "sqlite": "REAL"},
    "bool": {"postgresql": "BOOLEAN", "mysql": "BOOLEAN", "sqlite": "INTEGER"},
    "object": {"postgresql": "TEXT", "mysql": "TEXT", "sqlite": "TEXT"},
    "string": {"postgresql": "TEXT", "mysql": "TEXT", "sqlite": "TEXT"},
    "datetime64[ns]": {"postgresql": "TIMESTAMP", "mysql": "DATETIME", "sqlite": "TEXT"},
    "datetime64[ns, UTC]": {"postgresql": "TIMESTAMPTZ", "mysql": "TIMESTAMP", "sqlite": "TEXT"},
    "timedelta64[ns]": {"postgresql": "INTERVAL", "mysql": "BIGINT", "sqlite": "TEXT"},
    "category": {"postgresql": "TEXT", "mysql": "TEXT", "sqlite": "TEXT"},
}


def _dtype_to_sql(dtype: str, dialect: str) -> str:
    """Convert a pandas dtype to a SQL type string for the given dialect."""
    mapping = _DTYPE_MAP.get(dtype)
    if mapping and dialect in mapping:
        return mapping[dialect]
    # Fallback: return TEXT for unknown types
    return "TEXT"


def _nested_dtype_to_sql(col: ColumnDef, dialect: str) -> str:
    """Convert a ColumnDef's nested type to a SQL type string.

    Falls back to the flat _dtype_to_sql for non-nested columns.

    Mapping rules:
        - StructType: JSONB (PostgreSQL), JSON (MySQL/SQLite)
        - ArrayType:  element[] (PostgreSQL), JSON (MySQL/SQLite)
        - MapType:    JSONB (PostgreSQL), JSON (MySQL/SQLite)
    """
    if col.struct_type:
        if dialect == "postgresql":
            return "JSONB"
        return "JSON"

    if col.array_type:
        if dialect == "postgresql":
            return f"{col.array_type.element_type}[]"
        return "JSON"

    if col.map_type:
        if dialect == "postgresql":
            return "JSONB"
        return "JSON"

    return _dtype_to_sql(col.dtype, dialect)


def generate_ddl(
    schema: Schema,
    table_name: str,
    dialect: str = "postgresql",
    if_not_exists: bool = True,
) -> str:
    """Generate a CREATE TABLE DDL statement.

    Args:
        schema: Schema to generate DDL for.
        table_name: Target table name.
        dialect: SQL dialect (``"postgresql"``, ``"mysql"``, ``"sqlite"``).
        if_not_exists: If True, add IF NOT EXISTS clause (supported by
            PostgreSQL and SQLite; ignored for MySQL).

    Returns:
        A CREATE TABLE SQL string.

    Raises:
        ValueError: If an unsupported dialect is provided.
    """
    dialect = dialect.lower()
    supported = {d.value for d in SQLDialect}
    if dialect not in supported:
        raise ValueError(
            f"Unsupported dialect '{dialect}'. "
            f"Supported: {sorted(supported)}"
        )

    col_defs: List[str] = []
    for col in schema.columns:
        sql_type = _nested_dtype_to_sql(col, dialect)
        not_null = " NOT NULL" if not col.nullable else ""
        default = ""
        if col.default is not None:
            if isinstance(col.default, str):
                default = f" DEFAULT '{col.default}'"
            else:
                default = f" DEFAULT {col.default}"
        col_defs.append(f"    {col.name} {sql_type}{not_null}{default}")

    cols_sql = ",\n".join(col_defs)

    if if_not_exists and dialect in ("postgresql", "sqlite"):
        exists_clause = "IF NOT EXISTS "
    else:
        exists_clause = ""

    return (
        f"CREATE TABLE {exists_clause}{table_name} (\n"
        f"{cols_sql}\n"
        f");"
    )

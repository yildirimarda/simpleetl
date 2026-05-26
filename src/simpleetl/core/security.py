"""
Security utilities for SimpleETL.

Provides PII detection and masking, column-level encryption,
audit logging for data access, and RBAC hook points.
"""

import hashlib
import json
import logging
import os
import platform
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Platform-specific file locking
if platform.system() == "Windows":
    import msvcrt  # noqa: PLC0415
else:
    import fcntl  # noqa: PLC0415

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PII Detection & Masking
# ---------------------------------------------------------------------------

# Common PII patterns (regex-based)
PII_PATTERNS = {
    "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "phone": r"\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
    "ssn": r"\d{3}-\d{2}-\d{4}",
    "credit_card": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}",
    "ip_address": r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",
}

# Column name heuristics for PII detection
PII_COLUMN_KEYWORDS = {
    "email": ["email", "e_mail", "mail"],
    "phone": ["phone", "tel", "mobile", "cell"],
    "ssn": ["ssn", "social_security", "social_sec"],
    "credit_card": ["credit_card", "cc_number", "card_number", "cc_num"],
    "ip_address": ["ip", "ip_address", "client_ip"],
    "name": ["first_name", "last_name", "full_name", "surname"],
    "address": ["address", "street", "city", "zip", "postal"],
    "date_of_birth": ["dob", "birth_date", "date_of_birth", "birthdate"],
}


def detect_pii_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Detect columns that likely contain PII based on column names.

    Args:
        df: Input DataFrame whose columns are inspected.

    Returns:
        Dict mapping PII type to list of matching column names.
    """
    result: Dict[str, List[str]] = {}
    columns_lower = {col: col.lower() for col in df.columns}

    for pii_type, keywords in PII_COLUMN_KEYWORDS.items():
        matched: List[str] = []
        for original, lower in columns_lower.items():
            for keyword in keywords:
                if keyword in lower:
                    matched.append(original)
                    break
        if matched:
            result[pii_type] = matched

    logger.debug("Detected PII columns: %s", result)
    return result


def detect_pii_values(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    sample_size: int = 100,
) -> Dict[str, Dict[str, int]]:
    """Detect PII values in DataFrame columns using regex patterns.

    Samples up to sample_size rows per column.

    Args:
        df: Input DataFrame.
        columns: Columns to check. If None, checks all string columns.
        sample_size: Maximum number of rows to sample per column.

    Returns:
        Dict mapping column name to {pii_type: match_count}.
    """
    if columns is None:
        columns = [col for col in df.columns if df[col].dtype == object]

    result: Dict[str, Dict[str, int]] = {}

    for col in columns:
        if col not in df.columns:
            continue

        series = df[col].dropna().astype(str).head(sample_size)
        if series.empty:
            continue

        col_hits: Dict[str, int] = {}
        for pii_type, pattern in PII_PATTERNS.items():
            count = series.str.contains(pattern, regex=True).sum()
            if count > 0:
                col_hits[pii_type] = int(count)

        if col_hits:
            result[col] = col_hits

    logger.debug("Detected PII values: %s", result)
    return result


def mask_pii(
    df: pd.DataFrame,
    columns: Dict[str, str],
    method: str = "redact",
) -> pd.DataFrame:
    """Mask PII values in specified DataFrame columns.

    Args:
        df: Input DataFrame.
        columns: Dict mapping column name to PII type
            (e.g., {"email": "email", "phone": "phone"}).
        method: Masking method - "redact" (replace with ***), "hash"
            (SHA-256), "partial" (show first/last chars), "tokenize"
            (replace with token).

    Returns:
        DataFrame with masked PII columns.

    Raises:
        ValueError: If an unsupported masking method is specified.
    """
    valid_methods = {"redact", "hash", "partial", "tokenize"}
    if method not in valid_methods:
        raise ValueError(
            f"Unsupported masking method '{method}'. "
            f"Choose from: {valid_methods}"
        )

    result = df.copy()

    for col, pii_type in columns.items():
        if col not in result.columns:
            continue

        if method == "redact":
            result[col] = result[col].apply(
                lambda v, _pt=pii_type: _mask_redact(v, _pt)
            )
        elif method == "hash":
            result[col] = result[col].apply(_mask_hash)
        elif method == "partial":
            result[col] = result[col].apply(
                lambda v, _pt=pii_type: _mask_partial(v, _pt)
            )
        elif method == "tokenize":
            result[col] = result[col].apply(
                lambda v, _pt=pii_type: _mask_tokenize(v, _pt)
            )

    return result


def _mask_redact(value: Any, pii_type: str) -> str:
    """Replace a value with a redaction placeholder."""
    if pd.isna(value):
        return value
    redact_map = {
        "email": "***@***.***",
        "phone": "***-***-****",
        "ssn": "***-**-****",
        "credit_card": "****-****-****-****",
        "ip_address": "***.***.***.***",
        "name": "****",
        "address": "****",
        "date_of_birth": "****-**-**",
    }
    return redact_map.get(pii_type, "****")


def _mask_hash(value: Any) -> str:
    """Hash a value using SHA-256."""
    if pd.isna(value):
        return value
    text = str(value)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_tokenize_cache: Dict[str, str] = {}
_tokenize_counter: int = 0


def _mask_tokenize(value: Any, pii_type: str) -> str:
    """Replace a value with a consistent token."""
    global _tokenize_counter  # noqa: PLW0603

    if pd.isna(value):
        return value

    text = str(value)
    cache_key = f"{pii_type}:{text}"

    if cache_key not in _tokenize_cache:
        _tokenize_counter += 1
        _tokenize_cache[cache_key] = (
            f"<{pii_type.upper()}_{_tokenize_counter}>"
        )

    return _tokenize_cache[cache_key]


def _reset_token_cache() -> None:
    """Reset the tokenization cache (used for testing)."""
    global _tokenize_counter  # noqa: PLW0603
    _tokenize_cache.clear()
    _tokenize_counter = 0


def _mask_partial(value: Any, pii_type: str) -> str:
    """Partially mask a value, preserving first/last characters."""
    if pd.isna(value):
        return value

    text = str(value)

    if pii_type == "email" and "@" in text:
        return mask_email(text)
    if pii_type == "phone":
        return mask_phone(text)
    if pii_type == "credit_card":
        return mask_credit_card(text)

    if len(text) <= 2:
        return text[0] + "***"

    return text[0] + "***" + text[-1]


def mask_email(email: str) -> str:
    """Mask an email: 'user@example.com' -> 'u***@example.com'.

    Args:
        email: The email address to mask.

    Returns:
        Masked email string.
    """
    if not isinstance(email, str) or "@" not in email:
        return "***@***.***"

    local, domain = email.split("@", 1)
    masked_local = local[0] + "***"
    return f"{masked_local}@{domain}"


def mask_phone(phone: str) -> str:
    """Mask a phone number: '+1234567890' -> '+******7890'.

    Args:
        phone: The phone number to mask.

    Returns:
        Masked phone string preserving last 4 digits.
    """
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 4:
        return "*" * len(digits)

    last_four = digits[-4:]
    prefix = "+" if phone.strip().startswith("+") else ""
    masked = prefix + "*" * (len(digits) - 4) + last_four
    return masked


def mask_credit_card(cc: str) -> str:
    """Mask a credit card: '1234567890123456' -> '************3456'.

    Args:
        cc: The credit card number to mask.

    Returns:
        Masked credit card string preserving last 4 digits.
    """
    digits = re.sub(r"\D", "", cc)
    if len(digits) < 4:
        return "*" * len(digits)

    last_four = digits[-4:]
    masked = "*" * (len(digits) - 4) + last_four
    return masked


# ---------------------------------------------------------------------------
# Column-level Encryption
# ---------------------------------------------------------------------------


class ColumnEncryptor:
    """Column-level encryption using Fernet (symmetric encryption).

    Uses ``cryptography.fernet.Fernet`` for encryption/decryption.
    If ``cryptography`` is not installed, raises ``ImportError`` with a
    helpful message.

    Example::

        encryptor = ColumnEncryptor()
        encrypted_df = encryptor.encrypt_column(df, "ssn")
        decrypted_df = encryptor.decrypt_column(encrypted_df, "ssn")
    """

    def __init__(self, key: Optional[bytes] = None):
        """Initialize with optional key. Generates key if not provided.

        Args:
            key: A Fernet-compatible key. If None, a new key is
                generated via :meth:`generate_key`.
        """
        try:
            from cryptography.fernet import Fernet  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "The 'cryptography' package is required for "
                "ColumnEncryptor. Install it with: "
                "pip install cryptography"
            ) from exc

        if key is None:
            key = self.generate_key()
        self._fernet = Fernet(key)

    @staticmethod
    def generate_key() -> bytes:
        """Generate a new encryption key.

        Returns:
            A URL-safe base64-encoded 32-byte key.
        """
        from cryptography.fernet import Fernet  # noqa: PLC0415
        return Fernet.generate_key()

    def encrypt_column(
        self, df: pd.DataFrame, column: str
    ) -> pd.DataFrame:
        """Encrypt a column's values.

        Args:
            df: Input DataFrame.
            column: Column name to encrypt.

        Returns:
            DataFrame with the specified column encrypted.
        """
        result = df.copy()
        if column not in result.columns:
            return result

        result[column] = result[column].apply(
            lambda v: self._fernet.encrypt(str(v).encode()).decode()
            if not pd.isna(v)
            else v
        )
        return result

    def decrypt_column(
        self, df: pd.DataFrame, column: str
    ) -> pd.DataFrame:
        """Decrypt a column's values.

        Args:
            df: Input DataFrame with encrypted column.
            column: Column name to decrypt.

        Returns:
            DataFrame with the specified column decrypted.
        """
        result = df.copy()
        if column not in result.columns:
            return result

        result[column] = result[column].apply(
            lambda v: self._fernet.decrypt(str(v).encode()).decode()
            if not pd.isna(v)
            else v
        )
        return result


# ---------------------------------------------------------------------------
# Audit Logging
# ---------------------------------------------------------------------------


class AuditLogger:
    """Audit logger for data access tracking.

    Records who accessed what data, when, and what operations were
    performed. Entries are stored in memory and optionally written to
    a JSON-lines log file.

    Example::

        audit = AuditLogger(log_file="/var/log/audit.jsonl")
        audit.log_access("alice", "read", "customers")
        trail = audit.get_audit_trail()
    """

    def __init__(self, log_file: Optional[str] = None):
        """Initialize the audit logger.

        Args:
            log_file: Optional path to a JSON-lines file for
                persistent audit records.
        """
        self._entries: List[Dict[str, Any]] = []
        self._log_file = log_file

    def log_access(
        self,
        user: str,
        action: str,
        source: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a data access event.

        Args:
            user: Identifier of the user performing the action.
            action: Action performed (e.g., 'read', 'write', 'delete').
            source: Data source that was accessed.
            details: Optional additional details.
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "access",
            "user": user,
            "action": action,
            "source": source,
            "details": details or {},
        }
        self._entries.append(entry)
        logger.info(
            "Audit: user=%s action=%s source=%s", user, action, source
        )
        if self._log_file:
            self._write_to_file(entry)

    def log_transformation(
        self,
        user: str,
        job_name: str,
        operation: str,
        source: str,
        destination: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a data transformation event.

        Args:
            user: Identifier of the user running the transformation.
            job_name: Name of the ETL job performing the transform.
            operation: Transformation operation (e.g., 'mask', 'filter').
            source: Source data reference.
            destination: Destination data reference.
            details: Optional additional details.
        """
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "transformation",
            "user": user,
            "job_name": job_name,
            "operation": operation,
            "source": source,
            "destination": destination,
            "details": details or {},
        }
        self._entries.append(entry)
        logger.info(
            "Audit: user=%s job=%s operation=%s",
            user,
            job_name,
            operation,
        )
        if self._log_file:
            self._write_to_file(entry)

    def get_audit_trail(
        self,
        source: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve audit trail entries, optionally filtered.

        Args:
            source: Filter by data source.
            start_time: Include only entries at or after this time.
            end_time: Include only entries at or before this time.

        Returns:
            List of audit entry dicts matching the filters.
        """
        results = list(self._entries)

        if source is not None:
            results = [e for e in results if e["source"] == source]

        if start_time is not None:
            results = [
                e
                for e in results
                if datetime.fromisoformat(e["timestamp"]) >= start_time
            ]

        if end_time is not None:
            results = [
                e
                for e in results
                if datetime.fromisoformat(e["timestamp"]) <= end_time
            ]

        return results

    def _write_to_file(self, entry: Dict[str, Any]) -> None:
        """Append an audit entry to the log file as JSON.

        Uses platform-specific file locking (``fcntl`` on Unix,
        ``msvcrt`` on Windows) to ensure safe concurrent writes.
        """
        if self._log_file is None:
            return
        try:
            parent = os.path.dirname(self._log_file)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(self._log_file, "a") as fh:  # noqa: PTH123
                # Acquire an exclusive lock for safe concurrent writes
                if platform.system() == "Windows":
                    # Lock one byte at the end of file region
                    try:
                        msvcrt.fileno(fh)  # type: ignore[attr-defined]
                        msvcrt.locking(  # type: ignore[attr-defined]
                            msvcrt.fileno(fh),  # type: ignore[attr-defined]
                            msvcrt.LK_LOCK,  # type: ignore[attr-defined]
                            1,  # Lock 1 byte
                        )
                        fh.write(json.dumps(entry) + "\n")
                        fh.flush()
                    finally:
                        try:
                            msvcrt.locking(  # type: ignore[attr-defined]
                                msvcrt.fileno(fh),  # type: ignore[attr-defined]
                                msvcrt.LK_UNLCK,  # type: ignore[attr-defined]
                                1,
                            )
                        except OSError:
                            pass
                else:
                    try:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                        fh.write(json.dumps(entry) + "\n")
                        fh.flush()
                    finally:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError as exc:
            logger.error("Failed to write audit log: %s", exc)

    @classmethod
    def from_file(
        cls, path: str, log_file: Optional[str] = None
    ) -> "AuditLogger":
        """Load audit entries from a JSON-lines file.

        Returns a new ``AuditLogger`` instance populated with
        entries from the file.

        Args:
            path: File path to load audit entries from.
            log_file: Optional path to use as the audit log file
                for future writes.

        Returns:
            A new ``AuditLogger`` with loaded entries.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        audit_logger = cls(log_file=log_file)
        with open(path) as fh:  # noqa: PTH123
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                audit_logger._entries.append(entry)
        logger.info(
            "Loaded %d audit entries from %s",
            len(audit_logger._entries),
            path,
        )
        return audit_logger


# ---------------------------------------------------------------------------
# RBAC Hooks
# ---------------------------------------------------------------------------


class RBACPolicy:
    """Role-based access control policy.

    Defines roles, permissions, and column-level access rules.

    Example::

        policy = RBACPolicy()
        policy.add_role(
            "analyst",
            permissions=["read"],
            allowed_columns={"customers": ["id", "city"]},
        )
        policy.check_access("analyst", "read")
    """

    def __init__(self):
        """Initialize an empty policy."""
        self._roles: Dict[str, Dict[str, Any]] = {}

    def add_role(
        self,
        name: str,
        permissions: List[str],
        allowed_columns: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """Add a role with permissions and optional column-level access.

        Args:
            name: Role name.
            permissions: List of permissions (e.g., ['read', 'write',
                'transform']).
            allowed_columns: Dict mapping table/source to list of
                allowed columns.
        """
        self._roles[name] = {
            "permissions": set(permissions),
            "allowed_columns": allowed_columns or {},
        }
        logger.debug(
            "Added role '%s' with permissions: %s", name, permissions
        )

    def check_access(
        self,
        role: str,
        permission: str,
        source: Optional[str] = None,
        column: Optional[str] = None,
    ) -> bool:
        """Check if a role has a specific permission.

        Args:
            role: Role name to check.
            permission: Permission to verify (e.g., 'read').
            source: Optional source/table to check column access for.
            column: Optional column name to verify access to.

        Returns:
            True if the role has the specified permission (and
            column-level access, if provided).
        """
        if role not in self._roles:
            return False

        role_def = self._roles[role]
        if permission not in role_def["permissions"]:
            return False

        if source is not None and column is not None:
            allowed = role_def["allowed_columns"].get(source, [])
            if allowed and column not in allowed:
                return False

        return True

    def filter_columns(
        self,
        role: str,
        source: str,
        columns: List[str],
    ) -> List[str]:
        """Filter columns based on role's allowed columns for a source.

        Args:
            role: Role name.
            source: Source/table name.
            columns: Full list of column names to filter.

        Returns:
            Subset of columns the role is allowed to access.
        """
        if role not in self._roles:
            return []

        allowed = self._roles[role]["allowed_columns"].get(source, [])
        if not allowed:
            return columns

        return [c for c in columns if c in allowed]

    def save_to_file(self, path: str) -> None:
        """Save the RBAC policy to a JSON file.

        Serializes all roles, permissions, and column-level access
        rules to a JSON file. Creates parent directories if needed.

        Args:
            path: File path to save the policy to.
        """
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # Convert sets to lists for JSON serialization
        data = {
            role: {
                "permissions": sorted(info["permissions"]),
                "allowed_columns": info["allowed_columns"],
            }
            for role, info in self._roles.items()
        }
        with open(path, "w") as fh:  # noqa: PTH123
            json.dump(data, fh, indent=2, default=str)
        logger.info("Saved RBAC policy to %s", path)

    @classmethod
    def load_from_file(cls, path: str) -> "RBACPolicy":
        """Load an RBAC policy from a JSON file.

        Returns a new ``RBACPolicy`` instance populated with
        roles from the file.

        Args:
            path: File path to load the policy from.

        Returns:
            A new ``RBACPolicy`` with loaded roles.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        policy = cls()
        with open(path) as fh:  # noqa: PTH123
            data = json.load(fh)
        for role_name, role_info in data.items():
            policy.add_role(
                name=role_name,
                permissions=role_info.get("permissions", []),
                allowed_columns=role_info.get("allowed_columns", {}),
            )
        logger.info(
            "Loaded RBAC policy with %d roles from %s",
            len(policy._roles),
            path,
        )
        return policy


def apply_rbac_filter(
    df: pd.DataFrame,
    role: str,
    source: str,
    policy: RBACPolicy,
) -> pd.DataFrame:
    """Apply RBAC column filtering to a DataFrame.

    Removes columns the role does not have access to.

    Args:
        df: Input DataFrame.
        role: Role name to apply.
        source: Source/table name.
        policy: The RBAC policy to evaluate.

    Returns:
        DataFrame with unauthorized columns removed.
    """
    allowed = policy.filter_columns(role, source, list(df.columns))
    return df[allowed]

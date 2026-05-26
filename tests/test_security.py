"""
Tests for the security module.
"""

import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
import pandas as pd

from simpleetl.core.security import (
    ColumnEncryptor,
    AuditLogger,
    RBACPolicy,
    detect_pii_columns,
    detect_pii_values,
    mask_pii,
    mask_email,
    mask_phone,
    mask_credit_card,
    apply_rbac_filter,
    _reset_token_cache,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pii_df():
    """A DataFrame containing PII columns and values."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "email": [
            "alice@example.com",
            "bob@test.org",
            "charlie@demo.net",
            "diana@work.com",
            "eve@mail.io",
        ],
        "phone": [
            "+1234567890",
            "+0987654321",
            "555-123-4567",
            "8005551234",
            "+44 20 7946 0958",
        ],
        "ssn": [
            "123-45-6789",
            "987-65-4321",
            "111-22-3333",
            "444-55-6666",
            "777-88-9999",
        ],
        "credit_card": [
            "1234567890123456",
            "9876543210987654",
            "1234-5678-9012-3456",
            "9876 5432 1098 7654",
            "1111222233334444",
        ],
        "ip_address": [
            "192.168.1.1",
            "10.0.0.1",
            "172.16.0.1",
            "8.8.8.8",
            "1.1.1.1",
        ],
        "first_name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "last_name": ["Smith", "Jones", "Brown", "Davis", "Wilson"],
        "address": [
            "123 Main St",
            "456 Oak Ave",
            "789 Pine Rd",
            "321 Elm St",
            "654 Maple Dr",
        ],
        "date_of_birth": [
            "1990-01-15",
            "1985-06-20",
            "1992-11-30",
            "1988-03-25",
            "1995-09-10",
        ],
        "score": [88.5, 92.0, 76.3, 95.1, 81.7],
    })


@pytest.fixture
def simple_df():
    """A simple DataFrame for basic tests."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10, 20, 30],
    })


# ---------------------------------------------------------------------------
# PII Column Detection
# ---------------------------------------------------------------------------


class TestDetectPiiColumns:
    """Tests for detect_pii_columns."""

    def test_detects_email_column(self, pii_df):
        result = detect_pii_columns(pii_df)
        assert "email" in result
        assert "email" in result["email"]

    def test_detects_phone_column(self, pii_df):
        result = detect_pii_columns(pii_df)
        assert "phone" in result
        assert "phone" in result["phone"]

    def test_detects_name_columns(self, pii_df):
        result = detect_pii_columns(pii_df)
        assert "name" in result
        assert "first_name" in result["name"]
        assert "last_name" in result["name"]

    def test_detects_address_column(self, pii_df):
        result = detect_pii_columns(pii_df)
        assert "address" in result
        assert "address" in result["address"]

    def test_detects_dob_column(self, pii_df):
        result = detect_pii_columns(pii_df)
        assert "date_of_birth" in result
        assert "date_of_birth" in result["date_of_birth"]

    def test_no_pii_in_clean_df(self, simple_df):
        result = detect_pii_columns(simple_df)
        assert result == {}

    def test_case_insensitive_matching(self):
        df = pd.DataFrame({"EMAIL_ADDR": [1], "PhoneNumber": [2]})
        result = detect_pii_columns(df)
        assert "email" in result
        assert "phone" in result


# ---------------------------------------------------------------------------
# PII Value Detection
# ---------------------------------------------------------------------------


class TestDetectPiiValues:
    """Tests for detect_pii_values."""

    def test_detects_email_values(self, pii_df):
        result = detect_pii_values(pii_df, columns=["email"])
        assert "email" in result
        assert "email" in result["email"]
        assert result["email"]["email"] == 5

    def test_detects_credit_card_values(self, pii_df):
        result = detect_pii_values(pii_df, columns=["credit_card"])
        assert "credit_card" in result
        assert "credit_card" in result["credit_card"]

    def test_detects_ip_values(self, pii_df):
        result = detect_pii_values(pii_df, columns=["ip_address"])
        assert "ip_address" in result
        assert "ip_address" in result["ip_address"]

    def test_no_pii_in_numeric_column(self, pii_df):
        result = detect_pii_values(pii_df, columns=["score"])
        assert result == {}

    def test_sample_size_limits_rows(self, pii_df):
        result = detect_pii_values(
            pii_df, columns=["email"], sample_size=2
        )
        assert result["email"]["email"] == 2


# ---------------------------------------------------------------------------
# Masking Functions
# ---------------------------------------------------------------------------


class TestMaskEmail:
    """Tests for mask_email."""

    def test_masks_standard_email(self):
        assert mask_email("user@example.com") == "u***@example.com"

    def test_preserves_domain(self):
        result = mask_email("alice@test.org")
        assert result.endswith("@test.org")

    def test_single_char_local(self):
        result = mask_email("a@domain.com")
        assert result == "a***@domain.com"

    def test_invalid_email(self):
        assert mask_email("not-an-email") == "***@***.***"


class TestMaskPhone:
    """Tests for mask_phone."""

    def test_masks_phone_preserving_last_four(self):
        result = mask_phone("+1234567890")
        assert result.endswith("7890")

    def test_preserves_plus_prefix(self):
        result = mask_phone("+1234567890")
        assert result.startswith("+")

    def test_short_phone(self):
        result = mask_phone("123")
        assert result == "***"


class TestMaskCreditCard:
    """Tests for mask_credit_card."""

    def test_masks_credit_card_preserving_last_four(self):
        result = mask_credit_card("1234567890123456")
        assert result.endswith("3456")

    def test_all_asterisks_except_last_four(self):
        result = mask_credit_card("1234567890123456")
        assert result == "************3456"

    def test_short_cc(self):
        result = mask_credit_card("123")
        assert result == "***"


# ---------------------------------------------------------------------------
# mask_pii with different methods
# ---------------------------------------------------------------------------


class TestMaskPii:
    """Tests for the mask_pii function."""

    def test_redact_method(self, pii_df):
        result = mask_pii(
            pii_df, columns={"email": "email"}, method="redact"
        )
        assert all(v == "***@***.***" for v in result["email"])

    def test_hash_method(self, pii_df):
        result = mask_pii(
            pii_df, columns={"email": "email"}, method="hash"
        )
        # SHA-256 produces 64-char hex strings
        assert all(len(str(v)) == 64 for v in result["email"])

    def test_partial_method_email(self, pii_df):
        result = mask_pii(
            pii_df, columns={"email": "email"}, method="partial"
        )
        assert result["email"].iloc[0] == "a***@example.com"

    def test_tokenize_method(self, pii_df):
        _reset_token_cache()
        result = mask_pii(
            pii_df, columns={"email": "email"}, method="tokenize"
        )
        # All values should be tokenized
        assert all(
            str(v).startswith("<EMAIL_") for v in result["email"]
        )

    def test_invalid_method_raises(self, pii_df):
        with pytest.raises(ValueError, match="Unsupported masking method"):
            mask_pii(pii_df, columns={"email": "email"}, method="invalid")

    def test_missing_column_skipped(self, pii_df):
        result = mask_pii(
            pii_df,
            columns={"nonexistent": "email"},
            method="redact",
        )
        pd.testing.assert_frame_equal(result, pii_df)

    def test_multiple_columns(self, pii_df):
        result = mask_pii(
            pii_df,
            columns={"email": "email", "phone": "phone"},
            method="redact",
        )
        assert all(v == "***@***.***" for v in result["email"])
        assert all(v == "***-***-****" for v in result["phone"])


# ---------------------------------------------------------------------------
# ColumnEncryptor
# ---------------------------------------------------------------------------


class TestColumnEncryptor:
    """Tests for ColumnEncryptor."""

    def test_generate_key(self):
        key = ColumnEncryptor.generate_key()
        assert isinstance(key, bytes)
        assert len(key) > 0

    def test_encrypt_decrypt_round_trip(self, simple_df):
        encryptor = ColumnEncryptor()
        encrypted = encryptor.encrypt_column(simple_df, "name")
        decrypted = encryptor.decrypt_column(encrypted, "name")
        pd.testing.assert_series_equal(decrypted["name"], simple_df["name"])

    def test_encrypted_values_differ_from_original(self, simple_df):
        encryptor = ColumnEncryptor()
        encrypted = encryptor.encrypt_column(simple_df, "name")
        for orig, enc in zip(simple_df["name"], encrypted["name"]):
            assert str(orig) != str(enc)

    def test_encrypt_missing_column(self, simple_df):
        encryptor = ColumnEncryptor()
        result = encryptor.encrypt_column(simple_df, "nonexistent")
        pd.testing.assert_frame_equal(result, simple_df)

    def test_decrypt_missing_column(self, simple_df):
        encryptor = ColumnEncryptor()
        result = encryptor.decrypt_column(simple_df, "nonexistent")
        pd.testing.assert_frame_equal(result, simple_df)

    def test_custom_key(self):
        key = ColumnEncryptor.generate_key()
        encryptor = ColumnEncryptor(key=key)
        assert encryptor is not None

    def test_encrypt_preserves_nulls(self):
        df = pd.DataFrame({"col": ["a", None, "b"]})
        encryptor = ColumnEncryptor()
        encrypted = encryptor.encrypt_column(df, "col")
        assert pd.isna(encrypted["col"].iloc[1])


# ---------------------------------------------------------------------------
# AuditLogger
# ---------------------------------------------------------------------------


class TestAuditLogger:
    """Tests for AuditLogger."""

    def test_log_access_stores_entry(self):
        audit = AuditLogger()
        audit.log_access("alice", "read", "customers")
        trail = audit.get_audit_trail()
        assert len(trail) == 1
        assert trail[0]["user"] == "alice"
        assert trail[0]["action"] == "read"
        assert trail[0]["source"] == "customers"
        assert trail[0]["event_type"] == "access"

    def test_log_transformation_stores_entry(self):
        audit = AuditLogger()
        audit.log_transformation(
            "bob", "etl_job", "mask", "raw", "masked"
        )
        trail = audit.get_audit_trail()
        assert len(trail) == 1
        assert trail[0]["event_type"] == "transformation"
        assert trail[0]["job_name"] == "etl_job"
        assert trail[0]["operation"] == "mask"

    def test_filter_by_source(self):
        audit = AuditLogger()
        audit.log_access("alice", "read", "customers")
        audit.log_access("bob", "write", "orders")
        trail = audit.get_audit_trail(source="customers")
        assert len(trail) == 1
        assert trail[0]["user"] == "alice"

    def test_filter_by_time_range(self):
        audit = AuditLogger()
        now = datetime.now(timezone.utc)
        audit.log_access("alice", "read", "customers")
        start = now - timedelta(seconds=1)
        end = now + timedelta(seconds=1)
        trail = audit.get_audit_trail(start_time=start, end_time=end)
        assert len(trail) == 1

    def test_log_file_output(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonl", delete=False
        ) as f:
            path = f.name
        try:
            audit = AuditLogger(log_file=path)
            audit.log_access("alice", "read", "customers")
            with open(path) as fh:
                lines = fh.readlines()
            assert len(lines) == 1
        finally:
            os.unlink(path)

    def test_multiple_entries(self):
        audit = AuditLogger()
        audit.log_access("alice", "read", "customers")
        audit.log_access("bob", "write", "orders")
        audit.log_transformation(
            "alice", "job1", "mask", "raw", "masked"
        )
        trail = audit.get_audit_trail()
        assert len(trail) == 3


# ---------------------------------------------------------------------------
# RBACPolicy
# ---------------------------------------------------------------------------


class TestRBACPolicy:
    """Tests for RBACPolicy."""

    def test_add_and_check_role(self):
        policy = RBACPolicy()
        policy.add_role("admin", permissions=["read", "write", "delete"])
        assert policy.check_access("admin", "read") is True
        assert policy.check_access("admin", "write") is True
        assert policy.check_access("admin", "delete") is True

    def test_check_unknown_role(self):
        policy = RBACPolicy()
        assert policy.check_access("ghost", "read") is False

    def test_check_missing_permission(self):
        policy = RBACPolicy()
        policy.add_role("viewer", permissions=["read"])
        assert policy.check_access("viewer", "write") is False

    def test_column_level_access(self):
        policy = RBACPolicy()
        policy.add_role(
            "analyst",
            permissions=["read"],
            allowed_columns={"customers": ["id", "name"]},
        )
        assert (
            policy.check_access("analyst", "read", "customers", "id")
            is True
        )
        assert (
            policy.check_access("analyst", "read", "customers", "ssn")
            is False
        )

    def test_filter_columns(self):
        policy = RBACPolicy()
        policy.add_role(
            "analyst",
            permissions=["read"],
            allowed_columns={"customers": ["id", "name"]},
        )
        result = policy.filter_columns(
            "analyst", "customers", ["id", "name", "ssn", "email"]
        )
        assert result == ["id", "name"]

    def test_filter_columns_no_restrictions(self):
        policy = RBACPolicy()
        policy.add_role("admin", permissions=["read"])
        cols = ["id", "name", "ssn"]
        result = policy.filter_columns("admin", "customers", cols)
        assert result == cols

    def test_filter_columns_unknown_role(self):
        policy = RBACPolicy()
        result = policy.filter_columns("ghost", "customers", ["id", "name"])
        assert result == []

    def test_apply_rbac_filter(self, simple_df):
        policy = RBACPolicy()
        policy.add_role(
            "viewer",
            permissions=["read"],
            allowed_columns={"test_table": ["id", "name"]},
        )
        result = apply_rbac_filter(simple_df, "viewer", "test_table", policy)
        assert list(result.columns) == ["id", "name"]

    def test_apply_rbac_filter_no_restrictions(self, simple_df):
        policy = RBACPolicy()
        policy.add_role("admin", permissions=["read", "write"])
        result = apply_rbac_filter(
            simple_df, "admin", "test_table", policy
        )
        pd.testing.assert_frame_equal(result, simple_df)

"""
Tests for the configuration module.

Covers env var resolution, secrets resolution, config loading/saving,
and the new ETLJobConfig fields.
"""

import json
import os
import tempfile

import pytest
import yaml

from simpleetl.core.config import (
    ETLJobConfig,
    EnvVarResolutionError,
    load_config,
    resolve_env_vars,
    save_config,
)
from simpleetl.core.secrets import (
    EnvSecretsProvider,
    SecretNotFoundError,
    resolve_secrets,
)


# ---------------------------------------------------------------------------
# Original tests (kept intact)
# ---------------------------------------------------------------------------

def test_etl_job_config_creation():
    """Test creating an ETLJobConfig instance."""
    config = ETLJobConfig(
        name="test_job",
        description="A test job",
        platform="local",
        input_format="csv",
        output_format="csv",
    )

    assert config.name == "test_job"
    assert config.description == "A test job"
    assert config.platform == "local"
    assert config.input_format == "csv"
    assert config.output_format == "csv"


def test_load_config_yaml():
    """Test loading configuration from a YAML file."""
    config_data = {
        "name": "test_job",
        "description": "A test job from YAML",
        "platform": "local",
        "input_format": "csv",
        "output_format": "csv",
        "params": {
            "input_path": "test/input.csv",
            "output_path": "test/output.csv",
        },
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        yaml.dump(config_data, f)
        temp_path = f.name

    try:
        config = load_config(temp_path)
        assert config.name == "test_job"
        assert config.description == "A test job from YAML"
        assert config.platform == "local"
        assert config.input_format == "csv"
        assert config.output_format == "csv"
        assert config.params["input_path"] == "test/input.csv"
    finally:
        os.unlink(temp_path)


def test_load_config_json():
    """Test loading configuration from a JSON file."""
    config_data = {
        "name": "test_job",
        "description": "A test job from JSON",
        "platform": "local",
        "input_format": "csv",
        "output_format": "csv",
        "params": {
            "input_path": "test/input.csv",
            "output_path": "test/output.csv",
        },
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(config_data, f)
        temp_path = f.name

    try:
        config = load_config(temp_path)
        assert config.name == "test_job"
        assert config.description == "A test job from JSON"
        assert config.platform == "local"
        assert config.input_format == "csv"
        assert config.output_format == "csv"
        assert config.params["input_path"] == "test/input.csv"
    finally:
        os.unlink(temp_path)


def test_load_config_invalid_format():
    """Test loading configuration from an unsupported file format."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as f:
        f.write("invalid config")
        temp_path = f.name

    try:
        with pytest.raises(
            ValueError, match="Unsupported configuration file format"
        ):
            load_config(temp_path)
    finally:
        os.unlink(temp_path)


def test_load_config_nonexistent_file():
    """Test loading configuration from a nonexistent file."""
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


def test_save_config_yaml():
    """Test saving configuration to a YAML file."""
    config = ETLJobConfig(
        name="test_job",
        description="A test job",
        platform="local",
        input_format="csv",
        output_format="csv",
        params={"test_param": "test_value"},
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as f:
        temp_path = f.name

    try:
        save_config(config, temp_path)

        loaded_config = load_config(temp_path)
        assert loaded_config.name == "test_job"
        assert loaded_config.description == "A test job"
        assert loaded_config.platform == "local"
        assert loaded_config.input_format == "csv"
        assert loaded_config.output_format == "csv"
        assert loaded_config.params["test_param"] == "test_value"
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_save_config_json():
    """Test saving configuration to a JSON file."""
    config = ETLJobConfig(
        name="test_job",
        description="A test job",
        platform="local",
        input_format="csv",
        output_format="csv",
        params={"test_param": "test_value"},
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        temp_path = f.name

    try:
        save_config(config, temp_path)

        loaded_config = load_config(temp_path)
        assert loaded_config.name == "test_job"
        assert loaded_config.description == "A test job"
        assert loaded_config.platform == "local"
        assert loaded_config.input_format == "csv"
        assert loaded_config.output_format == "csv"
        assert loaded_config.params["test_param"] == "test_value"
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_etl_job_config_validation():
    """Test that ETLJobConfig validates required fields."""
    with pytest.raises(Exception):
        ETLJobConfig()

    config = ETLJobConfig(
        name="test_job", input_format="csv", output_format="csv"
    )
    assert config.name == "test_job"
    assert config.input_format == "csv"
    assert config.output_format == "csv"


# ---------------------------------------------------------------------------
# New ETLJobConfig field tests
# ---------------------------------------------------------------------------

def test_etl_job_config_secrets_provider_field():
    """Test the secrets_provider field on ETLJobConfig."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
        secrets_provider="env",
    )
    assert config.secrets_provider == "env"


def test_etl_job_config_env_prefix_field():
    """Test the env_prefix field on ETLJobConfig."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
        env_prefix="ETL_",
    )
    assert config.env_prefix == "ETL_"


def test_etl_job_config_defaults_for_new_fields():
    """Test that new fields default to None."""
    config = ETLJobConfig(
        name="test_job",
        input_format="csv",
        output_format="csv",
    )
    assert config.secrets_provider is None
    assert config.env_prefix is None


# ---------------------------------------------------------------------------
# resolve_env_vars tests
# ---------------------------------------------------------------------------

class TestResolveEnvVars:
    """Tests for the resolve_env_vars function."""

    def test_resolve_simple_var(self, monkeypatch):
        """Test resolving a simple ${VAR} reference."""
        monkeypatch.setenv("MY_VAR", "hello")
        assert resolve_env_vars("${MY_VAR}") == "hello"

    def test_resolve_var_with_default(self, monkeypatch):
        """Test resolving ${VAR:-default} when var is not set."""
        monkeypatch.delenv("UNSET_VAR", raising=False)
        result = resolve_env_vars("${UNSET_VAR:-fallback}")
        assert result == "fallback"

    def test_resolve_var_with_default_when_set(self, monkeypatch):
        """Test that ${VAR:-default} uses the env value when set."""
        monkeypatch.setenv("SET_VAR", "actual")
        result = resolve_env_vars("${SET_VAR:-fallback}")
        assert result == "actual"

    def test_resolve_missing_required_var(self, monkeypatch):
        """Test that a missing required var raises an error."""
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(EnvVarResolutionError, match="MISSING_VAR"):
            resolve_env_vars("${MISSING_VAR}")

    def test_resolve_bare_var(self, monkeypatch):
        """Test resolving $VAR syntax."""
        monkeypatch.setenv("BARE_VAR", "bare_value")
        assert resolve_env_vars("$BARE_VAR") == "bare_value"

    def test_resolve_bare_var_missing(self, monkeypatch):
        """Test that a missing bare var raises an error."""
        monkeypatch.delenv("MISSING_BARE", raising=False)
        with pytest.raises(EnvVarResolutionError, match="MISSING_BARE"):
            resolve_env_vars("$MISSING_BARE")

    def test_resolve_in_dict(self, monkeypatch):
        """Test recursive resolution in dicts."""
        monkeypatch.setenv("DB_HOST", "db.example.com")
        monkeypatch.setenv("DB_PORT", "5432")
        data = {
            "host": "${DB_HOST}",
            "port": "${DB_PORT}",
            "name": "mydb",
        }
        result = resolve_env_vars(data)
        assert result["host"] == "db.example.com"
        assert result["port"] == "5432"
        assert result["name"] == "mydb"

    def test_resolve_in_list(self, monkeypatch):
        """Test recursive resolution in lists."""
        monkeypatch.setenv("ITEM_A", "alpha")
        monkeypatch.setenv("ITEM_B", "beta")
        data = ["${ITEM_A}", "${ITEM_B}", "gamma"]
        result = resolve_env_vars(data)
        assert result == ["alpha", "beta", "gamma"]

    def test_resolve_nested_structures(self, monkeypatch):
        """Test resolution in deeply nested structures."""
        monkeypatch.setenv("NESTED_VAL", "deep")
        data = {
            "level1": {
                "level2": ["${NESTED_VAL}", {"level3": "${NESTED_VAL}"}]
            }
        }
        result = resolve_env_vars(data)
        assert result["level1"]["level2"][0] == "deep"
        assert result["level1"]["level2"][1]["level3"] == "deep"

    def test_resolve_non_string_passthrough(self):
        """Test that non-string, non-dict, non-list values pass through."""
        assert resolve_env_vars(42) == 42
        assert resolve_env_vars(True) is True
        assert resolve_env_vars(None) is None
        assert resolve_env_vars(3.14) == 3.14

    def test_resolve_empty_default(self, monkeypatch):
        """Test ${VAR:-} resolves to empty string when var is unset."""
        monkeypatch.delenv("EMPTY_DEFAULT", raising=False)
        result = resolve_env_vars("${EMPTY_DEFAULT:-}")
        assert result == ""

    def test_resolve_multiple_vars_in_one_string(self, monkeypatch):
        """Test resolving multiple vars in a single string."""
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "8080")
        result = resolve_env_vars("http://${HOST}:${PORT}/api")
        assert result == "http://localhost:8080/api"

    def test_resolve_plain_string_unchanged(self):
        """Test that a string without references is returned unchanged."""
        assert resolve_env_vars("just a string") == "just a string"


# ---------------------------------------------------------------------------
# Secrets resolution tests
# ---------------------------------------------------------------------------

class TestResolveSecrets:
    """Tests for the resolve_secrets function."""

    def test_resolve_secret_full_match(self, monkeypatch):
        """Test resolving a full ${secrets://env/NAME} reference."""
        monkeypatch.setenv("MY_SECRET", "s3cret")
        provider = EnvSecretsProvider()
        result = resolve_secrets("${secrets://env/MY_SECRET}", provider)
        assert result == "s3cret"

    def test_resolve_secret_missing(self):
        """Test that a missing secret raises SecretNotFoundError."""
        provider = EnvSecretsProvider()
        with pytest.raises(SecretNotFoundError):
            resolve_secrets("${secrets://env/NONEXISTENT}", provider)

    def test_resolve_secret_in_dict(self, monkeypatch):
        """Test recursive secret resolution in dicts."""
        monkeypatch.setenv("DB_PASS", "db_secret")
        monkeypatch.setenv("API_KEY", "key_secret")
        provider = EnvSecretsProvider()
        data = {
            "password": "${secrets://env/DB_PASS}",
            "api_key": "${secrets://env/API_KEY}",
            "name": "myapp",
        }
        result = resolve_secrets(data, provider)
        assert result["password"] == "db_secret"
        assert result["api_key"] == "key_secret"
        assert result["name"] == "myapp"

    def test_resolve_secret_in_list(self, monkeypatch):
        """Test recursive secret resolution in lists."""
        monkeypatch.setenv("S1", "one")
        monkeypatch.setenv("S2", "two")
        provider = EnvSecretsProvider()
        data = ["${secrets://env/S1}", "${secrets://env/S2}"]
        result = resolve_secrets(data, provider)
        assert result == ["one", "two"]

    def test_resolve_secret_non_string_passthrough(self):
        """Test that non-string values pass through unchanged."""
        provider = EnvSecretsProvider()
        assert resolve_secrets(42, provider) == 42
        assert resolve_secrets(None, provider) is None

    def test_resolve_secret_partial_string(self, monkeypatch):
        """Test secret resolution embedded in a larger string."""
        monkeypatch.setenv("TOKEN", "abc123")
        provider = EnvSecretsProvider()
        result = resolve_secrets(
            "Bearer ${secrets://env/TOKEN} end", provider
        )
        assert result == "Bearer abc123 end"


class TestEnvSecretsProvider:
    """Tests for the EnvSecretsProvider."""

    def test_get_existing_secret(self, monkeypatch):
        """Test retrieving an existing env var."""
        monkeypatch.setenv("TEST_SECRET", "value123")
        provider = EnvSecretsProvider()
        assert provider.get_secret("TEST_SECRET") == "value123"

    def test_get_missing_secret(self, monkeypatch):
        """Test that a missing env var raises SecretNotFoundError."""
        monkeypatch.setenv("FORGET_ME", "temp")
        monkeypatch.delenv("FORGET_ME")
        provider = EnvSecretsProvider()
        with pytest.raises(SecretNotFoundError, match="FORGET_ME"):
            provider.get_secret("FORGET_ME")


# ---------------------------------------------------------------------------
# Integration: load_config with env vars and secrets
# ---------------------------------------------------------------------------

class TestLoadConfigWithEnvVars:
    """Integration tests for load_config with env var resolution."""

    def _write_config(self, data: dict, suffix: str = ".yaml") -> str:
        """Helper to write a config dict to a temp file."""
        f = tempfile.NamedTemporaryFile(
            mode="w", suffix=suffix, delete=False
        )
        yaml.dump(data, f)
        f.close()
        return f.name

    def test_load_config_resolves_env_vars(self, monkeypatch):
        """Test that load_config resolves ${VAR} in config values."""
        monkeypatch.setenv("TEST_DB_HOST", "testhost")
        config_data = {
            "name": "env_job",
            "input_format": "csv",
            "output_format": "csv",
            "params": {"host": "${TEST_DB_HOST}"},
        }
        path = self._write_config(config_data)
        try:
            config = load_config(path)
            assert config.params["host"] == "testhost"
        finally:
            os.unlink(path)

    def test_load_config_resolves_defaults(self, monkeypatch):
        """Test that load_config uses defaults for unset vars."""
        monkeypatch.delenv("UNSET_DB_HOST", raising=False)
        config_data = {
            "name": "default_job",
            "input_format": "csv",
            "output_format": "csv",
            "params": {"host": "${UNSET_DB_HOST:-localhost}"},
        }
        path = self._write_config(config_data)
        try:
            config = load_config(path)
            assert config.params["host"] == "localhost"
        finally:
            os.unlink(path)

    def test_load_config_missing_required_var(self, monkeypatch):
        """Test that load_config raises for missing required vars."""
        monkeypatch.delenv("TOTALLY_MISSING", raising=False)
        config_data = {
            "name": "fail_job",
            "input_format": "csv",
            "output_format": "csv",
            "params": {"host": "${TOTALLY_MISSING}"},
        }
        path = self._write_config(config_data)
        try:
            with pytest.raises(EnvVarResolutionError):
                load_config(path)
        finally:
            os.unlink(path)

    def test_load_config_with_secrets_provider(self, monkeypatch):
        """Test that load_config resolves secrets when provider is given."""
        monkeypatch.setenv("MY_SECRET_PASSWORD", "p@ssw0rd")
        config_data = {
            "name": "secret_job",
            "input_format": "csv",
            "output_format": "csv",
            "params": {
                "password": "${secrets://env/MY_SECRET_PASSWORD}"
            },
        }
        path = self._write_config(config_data)
        try:
            config = load_config(path, secrets_provider=EnvSecretsProvider())
            assert config.params["password"] == "p@ssw0rd"
        finally:
            os.unlink(path)

    def test_load_config_env_prefix_auto_load(self, monkeypatch):
        """Test that env_prefix auto-loads matching env vars into params."""
        monkeypatch.setenv("ETL_BATCH_SIZE", "5000")
        monkeypatch.setenv("ETL_MAX_RETRIES", "5")
        config_data = {
            "name": "prefix_job",
            "input_format": "csv",
            "output_format": "csv",
            "env_prefix": "ETL_",
        }
        path = self._write_config(config_data)
        try:
            config = load_config(path)
            assert config.params["batch_size"] == "5000"
            assert config.params["max_retries"] == "5"
        finally:
            os.unlink(path)

    def test_load_config_env_prefix_does_not_override(self, monkeypatch):
        """Test that env_prefix does not override existing params."""
        monkeypatch.setenv("ETL_BATCH_SIZE", "9999")
        config_data = {
            "name": "prefix_job2",
            "input_format": "csv",
            "output_format": "csv",
            "env_prefix": "ETL_",
            "params": {"batch_size": "1000"},
        }
        path = self._write_config(config_data)
        try:
            config = load_config(path)
            assert config.params["batch_size"] == "1000"
        finally:
            os.unlink(path)

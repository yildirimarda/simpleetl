"""
Extended tests for secrets.py.

Covers: EnvSecretsProvider, resolve_secrets(), _SECRETS_PATTERN,
SecretNotFoundError, and edge cases.
"""

import pytest
import os

from simpleetl.core.secrets import (
    SecretsProvider,
    SecretNotFoundError,
    EnvSecretsProvider,
    resolve_secrets,
)


class TestSecretNotFoundError:
    def test_is_exception(self):
        err = SecretNotFoundError("test")
        assert isinstance(err, Exception)
        assert str(err) == "test"


class TestEnvSecretsProvider:
    def test_get_existing_secret(self):
        os.environ["TEST_SECRET_XYZ"] = "my_value"
        try:
            provider = EnvSecretsProvider()
            assert provider.get_secret("TEST_SECRET_XYZ") == "my_value"
        finally:
            del os.environ["TEST_SECRET_XYZ"]

    def test_get_missing_secret(self):
        provider = EnvSecretsProvider()
        with pytest.raises(SecretNotFoundError, match="not set"):
            provider.get_secret("NONEXISTENT_SECRET_XYZ")

    def test_empty_secret(self):
        os.environ["EMPTY_SECRET"] = ""
        try:
            provider = EnvSecretsProvider()
            assert provider.get_secret("EMPTY_SECRET") == ""
        finally:
            del os.environ["EMPTY_SECRET"]

    def test_secret_with_special_chars(self):
        os.environ["SPECIAL_SECRET"] = "p@$$w0rd!#%^&*()"
        try:
            provider = EnvSecretsProvider()
            assert provider.get_secret("SPECIAL_SECRET") == "p@$$w0rd!#%^&*()"
        finally:
            del os.environ["SPECIAL_SECRET"]


class TestResolveSecrets:
    def test_resolve_string_secret(self):
        os.environ["DB_PASSWORD"] = "s3cret"
        try:
            provider = EnvSecretsProvider()
            result = resolve_secrets("${secrets://env/DB_PASSWORD}", provider)
            assert result == "s3cret"
        finally:
            del os.environ["DB_PASSWORD"]

    def test_resolve_in_larger_string(self):
        os.environ["API_KEY"] = "key123"
        try:
            provider = EnvSecretsProvider()
            result = resolve_secrets(
                "https://api.example.com?key=${secrets://env/API_KEY}",
                provider,
            )
            assert result == "https://api.example.com?key=key123"
        finally:
            del os.environ["API_KEY"]

    def test_resolve_multiple_secrets_in_string(self):
        os.environ["USER"] = "admin"
        os.environ["PASS"] = "secret"
        try:
            provider = EnvSecretsProvider()
            result = resolve_secrets(
                "${secrets://env/USER}:${secrets://env/PASS}", provider
            )
            assert result == "admin:secret"
        finally:
            del os.environ["USER"]
            del os.environ["PASS"]

    def test_resolve_dict(self):
        os.environ["DB_USER"] = "admin"
        try:
            provider = EnvSecretsProvider()
            result = resolve_secrets(
                {
                    "host": "localhost",
                    "user": "${secrets://env/DB_USER}",
                    "port": 5432,
                },
                provider,
            )
            assert result == {
                "host": "localhost",
                "user": "admin",
                "port": 5432,
            }
        finally:
            del os.environ["DB_USER"]

    def test_resolve_list(self):
        os.environ["VAL1"] = "a"
        os.environ["VAL2"] = "b"
        try:
            provider = EnvSecretsProvider()
            result = resolve_secrets(
                ["${secrets://env/VAL1}", "${secrets://env/VAL2}", "plain"],
                provider,
            )
            assert result == ["a", "b", "plain"]
        finally:
            del os.environ["VAL1"]
            del os.environ["VAL2"]

    def test_resolve_nested_dict(self):
        os.environ["NESTED_SECRET"] = "deep"
        try:
            provider = EnvSecretsProvider()
            result = resolve_secrets(
                {"outer": {"inner": "${secrets://env/NESTED_SECRET}"}},
                provider,
            )
            assert result == {"outer": {"inner": "deep"}}
        finally:
            del os.environ["NESTED_SECRET"]

    def test_resolve_plain_string(self):
        provider = EnvSecretsProvider()
        result = resolve_secrets("no secrets here", provider)
        assert result == "no secrets here"

    def test_resolve_integer(self):
        provider = EnvSecretsProvider()
        result = resolve_secrets(42, provider)
        assert result == 42

    def test_resolve_none(self):
        provider = EnvSecretsProvider()
        result = resolve_secrets(None, provider)
        assert result is None

    def test_resolve_missing_secret_in_dict(self):
        provider = EnvSecretsProvider()
        with pytest.raises(SecretNotFoundError):
            resolve_secrets(
                {"key": "${secrets://env/NONEXISTENT_SECRET_XYZ}"},
                provider,
            )

    def test_resolve_missing_secret_in_list(self):
        provider = EnvSecretsProvider()
        with pytest.raises(SecretNotFoundError):
            resolve_secrets(
                ["${secrets://env/NONEXISTENT_SECRET_XYZ}"],
                provider,
            )

    def test_resolve_empty_string(self):
        provider = EnvSecretsProvider()
        result = resolve_secrets("", provider)
        assert result == ""

    def test_resolve_empty_dict(self):
        provider = EnvSecretsProvider()
        result = resolve_secrets({}, provider)
        assert result == {}

    def test_resolve_empty_list(self):
        provider = EnvSecretsProvider()
        result = resolve_secrets([], provider)
        assert result == []


class TestSecretsProviderAbstract:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            SecretsProvider()


class TestEnvSecretsProviderSubclass:
    def test_is_subclass(self):
        assert issubclass(EnvSecretsProvider, SecretsProvider)

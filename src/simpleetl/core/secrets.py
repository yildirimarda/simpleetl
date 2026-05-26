"""
Secrets provider abstraction for SimpleETL.

Supports resolving secrets from environment variables, AWS Secrets Manager,
Azure Key Vault, and HashiCorp Vault.
"""

import os
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class SecretsProvider(ABC):
    """Abstract base class for secrets providers."""

    @abstractmethod
    def get_secret(self, name: str) -> str:
        """
        Retrieve a secret value by name.

        Args:
            name: The secret identifier.

        Returns:
            The secret value as a string.

        Raises:
            SecretNotFoundError: If the secret does not exist.
        """
        ...


class SecretNotFoundError(Exception):
    """Raised when a requested secret cannot be found."""
    pass


class EnvSecretsProvider(SecretsProvider):
    """Read secrets from environment variables."""

    def get_secret(self, name: str) -> str:
        """
        Retrieve a secret from an environment variable.

        Args:
            name: The environment variable name.

        Returns:
            The value of the environment variable.

        Raises:
            SecretNotFoundError: If the environment variable is not set.
        """
        value = os.environ.get(name)
        if value is None:
            raise SecretNotFoundError(
                f"Environment variable '{name}' is not set"
            )
        return value


class AwsSecretsManagerProvider(SecretsProvider):
    """Read secrets from AWS Secrets Manager."""

    def __init__(
        self,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
    ):
        """
        Initialize the AWS Secrets Manager provider.

        Args:
            region_name: AWS region for the Secrets Manager client.
            profile_name: AWS profile name for credentials.
        """
        import boto3

        session_kwargs: Dict[str, Any] = {}
        if region_name:
            session_kwargs["region_name"] = region_name
        if profile_name:
            session_kwargs["profile_name"] = profile_name

        session = boto3.Session(**session_kwargs)
        self._client = session.client("secretsmanager")

    def get_secret(self, name: str) -> str:
        """
        Retrieve a secret from AWS Secrets Manager.

        Args:
            name: The secret name or ARN.

        Returns:
            The secret string value.

        Raises:
            SecretNotFoundError: If the secret cannot be retrieved.
        """
        import botocore.exceptions

        try:
            response = self._client.get_secret_value(SecretId=name)
            return response["SecretString"]
        except botocore.exceptions.ClientError as e:
            raise SecretNotFoundError(
                f"Failed to retrieve secret '{name}' from AWS Secrets Manager: "
                f"{e.response['Error']['Message']}"
            ) from e


class AzureKeyVaultProvider(SecretsProvider):
    """Read secrets from Azure Key Vault."""

    def __init__(self, vault_url: str):
        """
        Initialize the Azure Key Vault provider.

        Args:
            vault_url: The URL of the Azure Key Vault
                (e.g., 'https://my-vault.vault.azure.net/').
        """
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        credential = DefaultAzureCredential()
        self._client = SecretClient(vault_url=vault_url, credential=credential)

    def get_secret(self, name: str) -> str:
        """
        Retrieve a secret from Azure Key Vault.

        Args:
            name: The secret name.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret cannot be retrieved.
        """
        try:
            secret = self._client.get_secret(name)
            value = secret.value
            if value is None:
                raise SecretNotFoundError(
                    f"Secret '{name}' has no value in Azure Key Vault"
                )
            return value
        except SecretNotFoundError:
            raise
        except Exception as e:
            raise SecretNotFoundError(
                f"Failed to retrieve secret '{name}' from Azure Key Vault: {e}"
            ) from e


class HashiCorpVaultProvider(SecretsProvider):
    """Read secrets from HashiCorp Vault."""

    def __init__(
        self,
        url: str = "http://127.0.0.1:8200",
        token: Optional[str] = None,
    ):
        """
        Initialize the HashiCorp Vault provider.

        Args:
            url: The Vault server URL.
            token: The Vault authentication token.
        """
        import hvac

        self._client = hvac.Client(url=url, token=token)

    def get_secret(self, name: str) -> str:
        """
        Retrieve a secret from HashiCorp Vault (KV v2 engine).

        Args:
            name: The secret path (e.g., 'secret/data/myapp/config').

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret cannot be retrieved.
        """
        import hvac.exceptions

        try:
            response = self._client.secrets.kv.v2.read_secret_version(
                path=name
            )
            return response["data"]["data"]["value"]
        except hvac.exceptions.Forbidden as e:
            raise SecretNotFoundError(
                f"Access denied for secret '{name}' in HashiCorp Vault: {e}"
            ) from e
        except Exception as e:
            raise SecretNotFoundError(
                f"Failed to retrieve secret '{name}' from HashiCorp Vault: {e}"
            ) from e


# Regex pattern for ${secrets://provider/secret-name} syntax
_SECRETS_PATTERN = re.compile(
    r"\$\{secrets://([a-zA-Z0-9_-]+)/([^}]+)\}"
)


def resolve_secrets(
    value: Union[str, Dict, List, Any],
    provider: SecretsProvider,
) -> Union[str, Dict, List, Any]:
    """
    Resolve secret references in a value using the given provider.

    Supports the ``${secrets://provider/secret-name}`` syntax. Resolution is
    applied recursively to all string values in dicts and lists.

    Args:
        value: The value to resolve.  Strings are scanned for secret
            references; dicts and lists are recursed into.
        provider: The ``SecretsProvider`` instance used to fetch secrets.

    Returns:
        The value with all secret references replaced by their resolved
        values.

    Example::

        provider = EnvSecretsProvider()
        resolve_secrets("${secrets://env/DB_PASSWORD}", provider)
    """
    if isinstance(value, str):
        match = _SECRETS_PATTERN.fullmatch(value)
        if match:
            secret_name = match.group(2)
            return provider.get_secret(secret_name)
        # Also handle partial replacement within a larger string
        def _replace(match: re.Match) -> str:
            secret_name = match.group(2)
            return provider.get_secret(secret_name)

        return _SECRETS_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {
            key: resolve_secrets(val, provider)
            for key, val in value.items()
        }
    if isinstance(value, list):
        return [resolve_secrets(item, provider) for item in value]
    return value

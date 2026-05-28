"""
Configuration loading and validation for ETL jobs.

Supports environment variable interpolation (``${VAR}``, ``${VAR:-default}``)
and secret resolution via the :mod:`secrets` module.
"""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel

from simpleetl.core.secrets import (
    EnvSecretsProvider,
    SecretsProvider,
    resolve_secrets,
)

# Matches ${VAR}, ${VAR:-default}, and ${VAR:-} (empty default)
_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}")
# Matches $VAR (bare reference without braces)
_BARE_ENV_VAR_PATTERN = re.compile(r"\$([A-Z_][A-Z0-9_]*)")


class EnvVarResolutionError(Exception):
    """Raised when a required environment variable is not set."""
    pass


def resolve_env_vars(
    value: Union[str, Dict, List, Any],
) -> Union[str, Dict, List, Any]:
    """
    Resolve environment variable references in a value.

    Supports three syntaxes:

    * ``${VAR}`` — required; raises ``EnvVarResolutionError`` if not set.
    * ``${VAR:-default}`` — uses *default* when ``VAR`` is not set.
    * ``$VAR`` — required bare reference; raises if not set.

    Resolution is applied recursively to all string values in dicts and lists.

    Args:
        value: The value to resolve.

    Returns:
        The value with all environment variable references replaced.

    Raises:
        EnvVarResolutionError: If a required variable has no value and no
            default is provided.
    """
    if isinstance(value, str):
        return _resolve_env_vars_in_string(value)
    if isinstance(value, dict):
        return {k: resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_env_vars(item) for item in value]
    return value


def _resolve_env_vars_in_string(value: str) -> str:
    """Resolve all env var references within a single string."""

    def _replace_braced(match: re.Match) -> str:
        var_name = match.group(1)
        default = match.group(2)  # None if no :-default was provided
        env_value = os.environ.get(var_name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        raise EnvVarResolutionError(
            f"Environment variable '{var_name}' is not set and no default "
            f"is provided in config value: {value!r}"
        )

    def _replace_bare(match: re.Match) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is not None:
            return env_value
        raise EnvVarResolutionError(
            f"Environment variable '{var_name}' is not set in config value: "
            f"{value!r}"
        )

    # Process braced references first, then bare ones.
    result = _ENV_VAR_PATTERN.sub(_replace_braced, value)
    result = _BARE_ENV_VAR_PATTERN.sub(_replace_bare, result)
    return result


class DatabaseConfig(BaseModel):
    """Database connection configuration for ETL jobs."""

    url: Optional[str] = None
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600
    ssl_mode: Optional[str] = None
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    connect_timeout: int = 10
    read_timeout: int = 30
    write_timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 1.0


class ETLJobConfig(BaseModel):
    """Base configuration model for ETL jobs."""

    name: str
    description: Optional[str] = None
    platform: str = "local"
    input_format: str
    output_format: str
    max_retries: int = 0
    retry_delay: float = 1.0
    log_level: str = "INFO"
    params: Dict[str, Any] = {}
    secrets_provider: Optional[str] = None
    env_prefix: Optional[str] = None
    incremental: bool = False
    incremental_column: Optional[str] = None
    incremental_strategy: str = "watermark"
    watermark_store: str = "file"
    database: DatabaseConfig = DatabaseConfig()
    openlineage_url: Optional[str] = None
    openlineage_namespace: str = "simpleetl"
    format_options: Dict[str, Dict[str, Any]] = {}
    batch_size: int = 10000


def _apply_env_prefix(config_data: Dict[str, Any],
                      prefix: Optional[str]) -> Dict[str, Any]:
    """
    Auto-load environment variables with the given prefix into config params.

    For example, with ``env_prefix="ETL_"``, an environment variable
    ``ETL_BATCH_SIZE=500`` would be injected into ``config_data["params"]``
    as ``{"batch_size": "500"}``.

    Args:
        config_data: The raw configuration dictionary.
        prefix: The environment variable prefix to scan for.

    Returns:
        The (possibly mutated) config dictionary.
    """
    if not prefix:
        return config_data

    params: Dict[str, Any] = dict(config_data.get("params", {}))
    prefix_upper = prefix.upper()
    for key, value in os.environ.items():
        if key.startswith(prefix_upper):
            param_key = key[len(prefix_upper):].lower()
            if param_key not in params:
                params[param_key] = value

    if params:
        config_data["params"] = params

    return config_data


def load_config(
    config_path: str | Path,
    secrets_provider: Optional[SecretsProvider] = None,
) -> ETLJobConfig:
    """
    Load and validate ETL job configuration from a YAML or JSON file.

    The loading pipeline is:

    1. Parse the file into a raw dictionary.
    2. Apply ``env_prefix`` auto-loading (if configured).
    3. Resolve environment variable references (``${VAR}``,
       ``${VAR:-default}``).
    4. Resolve secret references (``${secrets://...}``) if a
       *secrets_provider* is supplied.
    5. Validate with Pydantic.

    Args:
        config_path: Path to the configuration file.
        secrets_provider: Optional secrets provider for resolving
            ``${secrets://...}`` references.

    Returns:
        Validated ETLJobConfig instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        EnvVarResolutionError: If a required env var is missing.
        ValidationError: If the configuration is invalid.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    with open(config_path, "r") as f:
        if config_path.suffix in [".yaml", ".yml"]:
            config_data = yaml.safe_load(f)
        elif config_path.suffix == ".json":
            import json

            config_data = json.load(f)
        else:
            raise ValueError(
                f"Unsupported configuration file format: "
                f"{config_path.suffix}. "
                "Supported formats are .yaml, .yml, .json"
            )

    if not isinstance(config_data, dict):
        raise ValueError("Configuration file must contain a mapping at the "
                         "top level")

    # Step 1: env_prefix auto-loading (before env var resolution so that
    # prefixed vars can also be referenced via ${VAR} syntax).
    env_prefix = config_data.get("env_prefix")
    config_data = _apply_env_prefix(config_data, env_prefix)

    # Step 2: resolve environment variable references
    config_data = resolve_env_vars(config_data)
    if not isinstance(config_data, dict):
        raise ValueError(
            "Configuration must be a mapping after env var resolution"
        )

    # Step 3: resolve secrets if a provider is available
    if secrets_provider is not None:
        config_data = resolve_secrets(config_data, secrets_provider)
    elif config_data.get("secrets_provider"):
        provider_name = config_data["secrets_provider"]
        if provider_name == "env":
            config_data = resolve_secrets(
                config_data, EnvSecretsProvider()
            )

    return ETLJobConfig(**config_data)


def save_config(config: ETLJobConfig, config_path: str | Path) -> None:
    """
    Save ETL job configuration to a YAML or JSON file.

    Args:
        config: ETLJobConfig instance to save.
        config_path: Path to the output configuration file.
    """
    config_path = Path(config_path)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    config_dict = config.model_dump(exclude_unset=True)

    with open(config_path, "w") as f:
        if config_path.suffix in [".yaml", ".yml"]:
            yaml.dump(config_dict, f, default_flow_style=False)
        elif config_path.suffix == ".json":
            import json

            json.dump(config_dict, f, indent=2)
        else:
            raise ValueError(
                f"Unsupported configuration file format: "
                f"{config_path.suffix}. "
                "Supported formats are .yaml, .yml, .json"
            )

"""
Configuration loading and validation for ETL jobs.

Supports environment variable interpolation (``${VAR}``, ``${VAR:-default}``),
Jinja2 template rendering (requires ``simpleetl[template]``), and secret
resolution via the :mod:`secrets` module.
"""

import os
import re
from datetime import date, datetime
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


class ConfigTemplateError(Exception):
    """Raised when Jinja2 template rendering fails or Jinja2 is not installed."""


def render_config_template(
    content: str,
    template_vars: Optional[Dict[str, Any]] = None,
) -> str:
    """Render a Jinja2 template string with built-in ETL variables.

    Built-in template namespaces available in every template:

    * ``env`` — ``os.environ`` (e.g. ``{{ env.HOME }}``)
    * ``now`` — current :class:`datetime` (e.g. ``{{ now.strftime('%Y-%m-%d') }}``)
    * ``today`` — today's date as an ISO string (e.g. ``{{ today }}``)
    * ``params`` — the caller-supplied *template_vars* dict

    Args:
        content: Raw template string (typically the YAML/JSON file contents).
        template_vars: Additional variables injected as ``params``.

    Returns:
        Rendered string.

    Raises:
        ConfigTemplateError: If Jinja2 is not installed or rendering fails.
    """
    try:
        from jinja2 import Environment, StrictUndefined, TemplateError
    except ImportError:
        raise ConfigTemplateError(
            "jinja2 is required for config template support. "
            "Install it with: pip install simpleetl[template]"
        )

    env = Environment(undefined=StrictUndefined)
    ctx: Dict[str, Any] = {
        "env": os.environ,
        "now": datetime.now(),
        "today": date.today().isoformat(),
        "params": template_vars or {},
    }
    if template_vars:
        ctx.update(template_vars)

    try:
        return env.from_string(content).render(**ctx)
    except TemplateError as exc:
        raise ConfigTemplateError(f"Failed to render config template: {exc}") from exc


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
            f"Environment variable '{var_name}' is not set in config value: {value!r}"
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
    backoff_base: float = 1.0
    breaker_threshold: int = 5


class SchemaDriftConfig(BaseModel):
    """Schema drift detection configuration.

    When enabled, the schema of extracted data is compared against the
    latest registered version in a schema registry after each extract.
    """

    enabled: bool = False
    registry_path: str = ".simpleetl/schema_registry"
    schema_name: Optional[str] = None
    on_drift: str = "warn"  # one of: fail, warn, evolve
    auto_register: bool = True


class TracingConfig(BaseModel):
    """OpenTelemetry tracing configuration.

    Requires ``simpleetl[otel]``. When *endpoint* is unset, spans are
    exported to the console (or kept in memory during tests).
    """

    enabled: bool = False
    service_name: str = "simpleetl"
    endpoint: Optional[str] = None


class ETLJobConfig(BaseModel):
    """Base configuration model for ETL jobs."""

    name: str
    description: Optional[str] = None
    platform: str = "local"
    input_format: str
    output_format: str
    max_retries: int = 0
    retry_delay: float = 1.0
    backoff_base: float = 1.0
    breaker_threshold: int = 5
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
    engine: str = "pandas"
    batch_size: int = 10000
    max_buffer_mb: float = 100.0
    validation_rules: List[Dict[str, Any]] = []
    schema_drift: SchemaDriftConfig = SchemaDriftConfig()
    tracing: TracingConfig = TracingConfig()
    metrics_enabled: bool = False
    provenance_enabled: bool = False
    provenance_record_id_column: str = "id"
    quality_checks: Optional[Dict[str, Any]] = None
    lineage_enabled: bool = False


def _apply_env_prefix(
    config_data: Dict[str, Any], prefix: Optional[str]
) -> Dict[str, Any]:
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
            param_key = key[len(prefix_upper) :].lower()
            if param_key not in params:
                params[param_key] = value

    if params:
        config_data["params"] = params

    return config_data


def load_config(
    config_path: str | Path,
    secrets_provider: Optional[SecretsProvider] = None,
    *,
    template_vars: Optional[Dict[str, Any]] = None,
) -> ETLJobConfig:
    """
    Load and validate ETL job configuration from a YAML or JSON file.

    The loading pipeline is:

    1. Read the file as raw text.
    2. *(Optional)* Render through Jinja2 when *template_vars* is provided
       or the file contains ``{{`` markers.  Requires ``simpleetl[template]``.
    3. Parse the rendered text as YAML or JSON.
    4. Apply ``env_prefix`` auto-loading (if configured).
    5. Resolve environment variable references (``${VAR}``,
       ``${VAR:-default}``).
    6. Resolve secret references (``${secrets://...}``) if a
       *secrets_provider* is supplied.
    7. Validate with Pydantic.

    Args:
        config_path: Path to the configuration file.
        secrets_provider: Optional secrets provider for resolving
            ``${secrets://...}`` references.
        template_vars: Variables injected into the Jinja2 template context
            as the ``params`` namespace.  When supplied, template rendering
            is always applied; when *None*, rendering is skipped unless the
            file content contains ``{{``.

    Returns:
        Validated ETLJobConfig instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ConfigTemplateError: If template rendering fails.
        EnvVarResolutionError: If a required env var is missing.
        ValidationError: If the configuration is invalid.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as f:
        raw_content = f.read()

    # Apply Jinja2 template rendering when requested or when the file looks
    # like a template (contains {{ ... }}).
    if template_vars is not None or "{{" in raw_content:
        raw_content = render_config_template(raw_content, template_vars)

    if config_path.suffix in [".yaml", ".yml"]:
        config_data = yaml.safe_load(raw_content)
    elif config_path.suffix == ".json":
        import json

        config_data = json.loads(raw_content)
    else:
        raise ValueError(
            f"Unsupported configuration file format: "
            f"{config_path.suffix}. "
            "Supported formats are .yaml, .yml, .json"
        )

    if not isinstance(config_data, dict):
        raise ValueError("Configuration file must contain a mapping at the top level")

    # Step 1: env_prefix auto-loading (before env var resolution so that
    # prefixed vars can also be referenced via ${VAR} syntax).
    env_prefix = config_data.get("env_prefix")
    config_data = _apply_env_prefix(config_data, env_prefix)

    # Step 2: resolve environment variable references
    config_data = resolve_env_vars(config_data)
    if not isinstance(config_data, dict):
        raise ValueError("Configuration must be a mapping after env var resolution")

    # Step 3: resolve secrets if a provider is available
    if secrets_provider is not None:
        config_data = resolve_secrets(config_data, secrets_provider)
    elif config_data.get("secrets_provider"):
        provider_name = config_data["secrets_provider"]
        if provider_name == "env":
            config_data = resolve_secrets(config_data, EnvSecretsProvider())

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

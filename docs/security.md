# Security Best Practices

SimpleETL is designed with security as a core concern. This document covers
security features, configuration, and best practices for production deployments.

## Error Messages

SimpleETL error messages are designed to avoid leaking sensitive information:

- **No secrets in errors**: Connection strings, passwords, and API keys are never
  included in exception messages or tracebacks.
- **Safe path display**: File paths in error messages do not contain credentials.
  URLs with embedded credentials (e.g., `postgresql://user:pass@host/db`) are
  redacted.

## Secrets in Logs

SimpleETL ensures that secrets are not written to logs:

- **Provider credentials**: AWS keys, Azure tokens, and Vault tokens are never logged.
- **Connection strings**: Database URLs and connection strings are never logged at
  `INFO` or `WARNING` levels. If debugging requires connection details, use
  `sqlalchemy.engine` logging at `DEBUG` level with caution.
- **Configuration**: When logging configuration, sensitive fields (passwords, tokens)
  are replaced with `***`.

## Secrets Management

Use a dedicated secrets provider rather than hardcoding credentials:

```python
from simpleetl.core.secrets import AWSSecretsProvider

provider = AWSSecretsProvider(region_name="us-east-1")
config = load_config("job.yaml", secrets_provider=provider)
```

Supported providers:

| Provider | Module | Extra Dependency |
|----------|--------|-----------------|
| AWS Secrets Manager | `simpleetl.core.secrets` | `simpleetl[aws]` |
| Azure Key Vault | `simpleetl.core.secrets` | `simpleetl[secrets]` |
| HashiCorp Vault | `simpleetl.core.secrets` | `simpleetl[secrets]` |
| Environment variables | `simpleetl.core.secrets` | (built-in) |

## Configuration Security

### Environment Variable Interpolation

Use environment variables to keep credentials out of config files:

```yaml
# config.yaml
database:
  url: "${DATABASE_URL}"
```

The `${VAR:-default}` syntax provides fallback values for optional settings
without exposing production credentials.

### Env Prefix Loading

SimpleETL can auto-load configuration from environment variables with a prefix:

```python
config = ETLJobConfig(
    name="my_job",
    input_format="csv",
    output_format="parquet",
    env_prefix="ETL_",
)
```

With `env_prefix="ETL_"`, the environment variable `ETL_BATCH_SIZE=500` is
injected into `config.params["batch_size"]`.

## Audit Logging

The `AuditLogger` tracks all access and changes:

```python
from simpleetl.core.security import AuditLogger

logger = AuditLogger()
logger.log_access("read", "s3://bucket/data.csv", user="etl_user")
logger.log_change("transform", "applied filter: age > 18", user="etl_user")
```

Audit events include timestamp, action, resource, and user for compliance.

## RBAC (Role-Based Access Control)

Define roles with specific permissions:

```python
from simpleetl.core.security import RBACPolicy, Role

admin = Role("admin", permissions=["read", "write", "execute", "configure"])
analyst = Role("analyst", permissions=["read"])

policy = RBACPolicy()
policy.add_role("alice", admin)
policy.add_role("bob", analyst)

policy.can("alice", "write")   # True
policy.can("bob", "write")     # False
```

## File Locking

SimpleETL uses platform-appropriate file locking for concurrent access:

- **Unix/Linux/macOS**: `fcntl.flock()` advisory locks
- **Windows**: `msvcrt.locking()` exclusive locks

File locks prevent concurrent writes to shared resources like checkpoint files
and watermark stores.

## Security Checklist

- [ ] Use a secrets provider (not hardcoded credentials)
- [ ] Enable audit logging for production jobs
- [ ] Configure RBAC policies for multi-user environments
- [ ] Run `pip-audit` in CI to detect vulnerable dependencies
- [ ] Set appropriate log levels (`INFO` or higher in production)
- [ ] Review configuration files for accidental credential exposure
- [ ] Use environment variable interpolation for sensitive values
- [ ] Enable SSL/TLS for database connections

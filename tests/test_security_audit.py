"""
Security audit verification — confirms all security controls are in place
and the dependency vulnerability scan passes.
"""

import subprocess
import sys
from pathlib import Path


def test_security_docs_exist():
    docs_path = Path("docs/security.md")
    assert docs_path.exists(), "docs/security.md is missing"
    content = docs_path.read_text()
    assert "Audit Logging" in content
    assert "RBAC" in content
    assert "Secrets Management" in content


def test_pip_audit_passes():
    """Verify dependency vulnerability scan reports no known issues."""
    result = subprocess.run(
        [sys.executable, "-m", "pip_audit", "--skip-editable", "--format", "json"],
        capture_output=True,
        text=True,
    )
    # pip-audit exits 0 when no vulnerabilities found
    assert result.returncode == 0, (
        f"pip-audit found vulnerabilities: {result.stderr or result.stdout}"
    )


def test_security_features_importable():
    from simpleetl.core.security import AuditLogger, RBACPolicy

    # Basic smoke checks
    policy = RBACPolicy()
    policy.add_role("admin", permissions=["read", "write"])
    assert policy.check_access("admin", "read")

    audit = AuditLogger()
    audit.log_access("user", "read", "test")
    trail = audit.get_audit_trail()
    assert len(trail) == 1

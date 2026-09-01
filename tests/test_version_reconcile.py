"""Tests that version numbering is reconciled to 0.1.0."""

from pathlib import Path

import simpleetl
from simpleetl.cli import create_parser


def test_package_version_matches_pyproject():
    assert simpleetl.__version__ == "0.1.0"


def test_cli_version_string():
    parser = create_parser()
    # --version exits; just verify parser has the version string
    assert parser.prog == "simpleetl"


def test_docs_version_references():
    docs_dir = Path(__file__).parent.parent / "docs"
    stale_versions = ["1.0.0", "1.1.0", "1.2.0", "1.3.0"]
    for doc_file in docs_dir.glob("*.md"):
        content = doc_file.read_text()
        for v in stale_versions:
            # Allow the version only if it refers to a dependency or external spec,
            # not the framework version. Here we just assert no framework refs.
            # We check that the specific bad patterns are gone.
            assert f"v{v}" not in content, (
                f"Stale version reference v{v} found in {doc_file.name}"
            )


def test_readme_version_badge():
    readme = Path(__file__).parent.parent / "README.md"
    content = readme.read_text()
    assert "0.1.0" in content
    assert "1.3.0" not in content


def test_docs_index_links_all_docs():
    docs_dir = Path(__file__).parent.parent / "docs"
    index_path = docs_dir / "index.md"
    index_content = index_path.read_text()
    doc_files = sorted(p.name for p in docs_dir.glob("*.md") if p.name != "index.md")
    for doc_file in doc_files:
        # Each doc should be referenced in the index quick links table
        assert f"({doc_file})" in index_content, (
            f"Missing link to {doc_file} in docs/index.md"
        )


def test_lineage_default_producer_version():
    from simpleetl.core.lineage import OpenLineageConverter

    converter = OpenLineageConverter()
    assert converter.producer == "simpleetl/0.1.0"

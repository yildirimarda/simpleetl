"""
Tests for the __main__ module (python -m simpleetl entry point).
"""

import runpy
from unittest.mock import patch


class TestMainModule:
    """Test the __main__ entry point."""

    def test_main_module_invokes_cli_main(self):
        """Test that __main__.py calls cli.main() when executed as __main__."""
        with patch("simpleetl.cli.main") as mock_main:
            # Run the __main__.py file as __main__ to trigger the if block
            runpy.run_path(
                "/Users/ardayildirim/Downloads/simpleetl/src/simpleetl/__main__.py",
                run_name="__main__",
            )
            mock_main.assert_called_once()

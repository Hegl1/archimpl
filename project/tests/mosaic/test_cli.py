#!/usr/bin/env python
"""Tests for `mosaic` package."""

from click.testing import CliRunner

from mosaic import cli


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main, ["--data-directory", "./data/kemper"], input="\\q\n")
    assert result.exit_code == 0
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0

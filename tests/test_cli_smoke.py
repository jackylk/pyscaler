"""Smoke tests — CLI registers, frameworks load, help works."""
from typer.testing import CliRunner

from pyscaler.cli import app
from pyscaler.frameworks.registry import available_frameworks, get_framework

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "pyscaler" in result.output


def test_help_lists_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("analyze", "convert", "verify", "run", "frameworks"):
        assert cmd in result.output


def test_frameworks_command_lists_ray_and_aura():
    result = runner.invoke(app, ["frameworks"])
    assert result.exit_code == 0
    assert "ray" in result.output
    assert "aura" in result.output


def test_framework_registry():
    names = available_frameworks()
    assert "ray" in names
    assert "aura" in names


def test_get_framework_ray():
    fw = get_framework("ray")
    assert fw.name == "ray"
    assert "ray" in fw.runtime_dependencies()[0]


def test_get_framework_unknown_raises():
    import pytest
    with pytest.raises(ValueError):
        get_framework("nonexistent")

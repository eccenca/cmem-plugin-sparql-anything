"""Tests for the jar bin utils."""

from pathlib import Path

import pytest

from cmem_plugin_sparql_anything import utils


@pytest.fixture
def jar_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the module path to a temporary installation folder."""
    monkeypatch.setattr(utils, "get_module_path", lambda: str(tmp_path))
    return tmp_path


@pytest.mark.usefixtures("jar_dir")
def test_has_jar_false_when_empty() -> None:
    """No jar present -> download is required."""
    assert utils.has_jar() is False


def test_has_jar_true_for_current_version(jar_dir: Path) -> None:
    """The current version jar present -> no download required."""
    (jar_dir / utils.JAR_FILE_NAME).touch()
    assert utils.has_jar() is True


def test_has_jar_false_when_only_old_version_present(jar_dir: Path) -> None:
    """Only an outdated jar present -> the new version must still be downloaded."""
    (jar_dir / "sparql-anything-0.9.0.jar").touch()
    assert utils.has_jar() is False

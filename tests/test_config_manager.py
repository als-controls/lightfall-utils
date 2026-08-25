"""Tests for the parameterized ConfigManager."""

from pydantic import BaseModel

from lightfall_utils.config import ConfigManager


class _Schema(BaseModel):
    model_config = {"extra": "allow"}
    answer: int = 42


def test_model_class_and_session_set():
    mgr = ConfigManager(model_class=_Schema, skip_standard_paths=True)
    assert mgr.model.answer == 42
    mgr.set("answer", 7)
    assert mgr.get("answer") == 7
    assert mgr.model.answer == 7


def test_validation_error_falls_back_to_defaults():
    mgr = ConfigManager(model_class=_Schema, skip_standard_paths=True)
    mgr.set("answer", "definitely-not-an-int")
    assert mgr.model.answer == 42  # fallback model
    assert mgr.validation_errors


def test_permissive_default_schema():
    mgr = ConfigManager(skip_standard_paths=True)
    mgr.set("anything.goes", True)
    assert mgr.get("anything.goes") is True


def test_app_name_controls_user_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("APPDATA", str(tmp_path))  # windows
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))  # posix
    mgr = ConfigManager(app_name="fooapp", skip_standard_paths=True)
    assert mgr.get_user_config_path().parent == tmp_path / "fooapp"


def test_defaults_path_layer_loaded(monkeypatch, tmp_path):
    monkeypatch.setenv("APPDATA", str(tmp_path / "user"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "user"))
    monkeypatch.setenv("PROGRAMDATA", str(tmp_path / "global"))
    defaults = tmp_path / "defaults"
    defaults.mkdir()
    (defaults / "application.yaml").write_text("ui:\n  theme: shipped\n", encoding="utf-8")
    mgr = ConfigManager(app_name="fooapp", defaults_path=defaults)
    assert mgr.get("ui.theme") == "shipped"


def test_persist_writes_user_file(monkeypatch, tmp_path):
    monkeypatch.setenv("APPDATA", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("PROGRAMDATA", str(tmp_path / "global"))
    mgr = ConfigManager(app_name="fooapp")
    mgr.set("ui.theme", "dark", persist=True)
    saved = tmp_path / "fooapp" / "application.yaml"
    assert saved.exists()
    assert "dark" in saved.read_text(encoding="utf-8")

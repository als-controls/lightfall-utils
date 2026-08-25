"""Tests for the priority-layered config machinery."""

from lightfall_utils.config import ConfigLayer, ConfigPriority, LayeredConfig


def test_layer_precedence_deep_merge():
    lc = LayeredConfig()
    lc.add_layer(
        ConfigLayer(
            name="defaults",
            priority=ConfigPriority.DEFAULTS,
            data={"ui": {"theme": "light", "size": 1}},
        )
    )
    lc.add_layer(
        ConfigLayer(name="user", priority=ConfigPriority.USER, data={"ui": {"theme": "dark"}})
    )
    assert lc.get("ui.theme") == "dark"  # higher priority wins
    assert lc.get("ui.size") == 1  # deep merge keeps sibling keys


def test_set_targets_highest_priority_mutable_layer():
    lc = LayeredConfig()
    lc.add_layer(ConfigLayer(name="base", priority=ConfigPriority.DEFAULTS, mutable=True))
    lc.add_layer(ConfigLayer(name="session", priority=ConfigPriority.SESSION, mutable=True))
    lc.set("a.b", 5)
    assert lc.get_layer("session").get("a.b") == 5
    assert lc.get_layer("base").get("a.b") is None


def test_from_file_missing_yields_empty_layer(tmp_path):
    layer = ConfigLayer.from_file(tmp_path / "nope.yaml")
    assert layer.data == {}


def test_layer_save_and_reload(tmp_path):
    path = tmp_path / "sub" / "cfg.yaml"
    layer = ConfigLayer(name="user", priority=ConfigPriority.USER, mutable=True, source=path)
    layer.set("ui.theme", "dark")
    layer.save()
    reloaded = ConfigLayer.from_file(path)
    assert reloaded.get("ui.theme") == "dark"

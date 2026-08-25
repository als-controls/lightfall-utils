"""Tests for the semantic theming system."""

import pytest

from lightfall_utils.theming import (
    ThemeDefinition,
    ThemeManager,
    ThemeProvider,
    ThemeRegistry,
)


class _TestTheme(ThemeProvider):
    @property
    def name(self) -> str:
        return "testtheme"

    @property
    def display_name(self) -> str:
        return "Test Theme"

    @property
    def is_dark(self) -> bool:
        return True

    def get_theme_definition(self) -> ThemeDefinition:
        return ThemeDefinition(primary="#123456")


@pytest.fixture(autouse=True)
def _reset_singletons():
    yield
    ThemeManager.reset()
    ThemeRegistry.reset()


def test_definition_state_color_defaults():
    d = ThemeDefinition(success="#111111", error="#222222")
    assert d.connected == "#111111"
    assert d.disconnected == "#222222"


def test_provider_introspection():
    data = _TestTheme().get_introspection_data()
    assert data["name"] == "testtheme"
    assert data["is_dark"] is True


def test_registry_register_and_system_pick():
    reg = ThemeRegistry.get_instance()
    theme = _TestTheme()
    assert reg.register(theme)
    assert reg.get("testtheme") is theme
    assert reg.get_theme_for_system(is_dark=True) is theme


def test_manager_applies_registered_theme(qapp):
    ThemeRegistry.get_instance().register(_TestTheme())
    mgr = ThemeManager.get_instance()
    mgr.set_theme_by_name("testtheme")
    assert mgr.colors.primary == "#123456"
    assert "#123456" in mgr.generate_stylesheet()


def test_stylesheet_contributor_hook(qapp):
    mgr = ThemeManager.get_instance()

    def contrib(colors, islands, font_size):
        return "/* CONTRIBUTED */"

    mgr.add_stylesheet_contributor(contrib)
    mgr.add_stylesheet_contributor(contrib)  # duplicate registration is a no-op
    assert mgr.generate_stylesheet().count("/* CONTRIBUTED */") == 1
    mgr.remove_stylesheet_contributor(contrib)
    assert "/* CONTRIBUTED */" not in mgr.generate_stylesheet()


def test_default_contributors_survive_reset(qapp):
    def contrib(colors, islands, font_size):
        return "/* DEFAULT-CONTRIB */"

    ThemeManager.default_stylesheet_contributors.append(contrib)
    try:
        assert "/* DEFAULT-CONTRIB */" in ThemeManager.get_instance().generate_stylesheet()
        ThemeManager.reset()
        assert "/* DEFAULT-CONTRIB */" in ThemeManager.get_instance().generate_stylesheet()
    finally:
        ThemeManager.default_stylesheet_contributors.remove(contrib)


def test_builtin_themes_provide_definitions():
    from lightfall_utils.theming import builtin

    for cls in (
        builtin.LightThemePlugin,
        builtin.SlateThemePlugin,
        builtin.DarkBlueThemePlugin,
        builtin.IslandsThemePlugin,
        builtin.CatppuccinMochaThemePlugin,
        builtin.EldritchThemePlugin,
        builtin.EvangelionThemePlugin,
        builtin.AyakaThemePlugin,
    ):
        theme = cls()
        assert theme.name
        assert isinstance(theme.get_theme_definition(), ThemeDefinition)


def test_islands_stylesheet_generation():
    from lightfall_utils.theming.builtin import generate_islands_stylesheet
    from lightfall_utils.theming.manager import LIGHT_COLORS

    css = generate_islands_stylesheet(LIGHT_COLORS)
    assert LIGHT_COLORS.primary in css

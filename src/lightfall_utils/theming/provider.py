"""Theme token definitions and the theme-provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class ThemeDefinition:
    """Color and style definitions for a theme.

    Attributes:
        primary: Primary brand/accent color.
        secondary: Secondary accent color.
        success: Success/positive state color.
        warning: Warning state color.
        error: Error/danger state color.
        info: Informational state color.
        background: Main background color.
        surface: Elevated surface color.
        text: Primary text color.
        text_secondary: Secondary/muted text color.
        border: Border/divider color.
        connected: Connected state color (defaults to success).
        disconnected: Disconnected state color (defaults to error).
        css_overrides: Optional CSS rules to append after base stylesheet.
    """

    primary: str = "#2563eb"  # Blue
    secondary: str = "#7c3aed"  # Purple
    success: str = "#16a34a"  # Green
    warning: str = "#d97706"  # Amber
    error: str = "#dc2626"  # Red
    info: str = "#0891b2"  # Cyan

    background: str = "#ffffff"
    surface: str = "#f3f4f6"
    text: str = "#1f2937"
    text_secondary: str = "#6b7280"
    border: str = "#e5e7eb"

    connected: str = ""
    disconnected: str = ""

    # Islands layout: "sea" is the visible gap behind floating panels.
    # When empty, falls back to background (non-Islands themes unchanged).
    sea: str = ""

    css_overrides: str = ""

    def __post_init__(self) -> None:
        """Set default state colors based on theme."""
        if not self.connected:
            self.connected = self.success
        if not self.disconnected:
            self.disconnected = self.error


class ThemeProvider(ABC):
    """A named theme that supplies a ThemeDefinition.

    Framework-free: host applications may mix this into their own plugin
    base classes (Lightfall mixes it with its PluginType).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier (lowercase, no spaces, e.g. "slate")."""
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name shown in a theme selector."""
        ...

    @property
    @abstractmethod
    def is_dark(self) -> bool:
        """Whether this is a dark theme (used for system-theme detection)."""
        ...

    @abstractmethod
    def get_theme_definition(self) -> ThemeDefinition:
        """Get the theme's color definitions."""
        ...

    def get_introspection_data(self) -> dict[str, Any]:
        """Machine-readable summary of this theme."""
        definition = self.get_theme_definition()
        return {
            "type": getattr(self, "type_name", "theme"),
            "name": self.name,
            "display_name": self.display_name,
            "is_dark": self.is_dark,
            "has_css_overrides": bool(definition.css_overrides),
            "class": self.__class__.__name__,
            "module": self.__class__.__module__,
        }

"""High-level configuration manager.

Wraps LayeredConfig with pydantic validation and automatic loading from
platform-standard locations. The validating model class, application
directory name, and bundled-defaults location are supplied by the host
application.

Standard configuration locations (in priority order):
    1. Bundled defaults (``defaults_path``, if given)
    2. Global config (``/etc/<app_name>/`` or ``%PROGRAMDATA%/<app_name>/``)
    3. User config (``~/.config/<app_name>/`` or ``%APPDATA%/<app_name>/``)
    4. Session overrides (runtime-only)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from lightfall_utils.config.layers import ConfigLayer, ConfigPriority, LayeredConfig
from lightfall_utils.logging import logger

if TYPE_CHECKING:
    from collections.abc import Sequence


class PermissiveModel(BaseModel):
    """Default schema: accepts any keys, validates nothing."""

    model_config = {"extra": "allow"}


def _user_config_dir(app_name: str) -> Path:
    """Per-user configuration directory for *app_name*."""
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / app_name


def _global_config_dir(app_name: str) -> Path:
    """System-wide configuration directory for *app_name*."""
    if os.name == "nt":
        return Path(os.environ.get("PROGRAMDATA", "C:/ProgramData")) / app_name
    return Path("/etc") / app_name


class ConfigManager:
    """Layered configuration with pydantic validation.

    Example:
        >>> config = ConfigManager(model_class=MyConfig, app_name="myapp")
        >>> config.get("ui.theme")
        "dark"
        >>> config.model.ui.theme
        "dark"
        >>> config.set("ui.theme", "light", persist=True)
    """

    DEFAULT_CONFIG_FILENAME = "application.yaml"

    def __init__(
        self,
        *,
        model_class: type[BaseModel] = PermissiveModel,
        app_name: str = "lightfall-utils",
        defaults_path: Path | str | None = None,
        default_filename: str | None = None,
        extra_paths: Sequence[Path | str] | None = None,
        skip_standard_paths: bool = False,
    ) -> None:
        """Initialize the ConfigManager.

        Args:
            model_class: Pydantic model the merged config validates against.
            app_name: Directory name under the platform config locations.
            defaults_path: Directory holding the bundled defaults file
                (loaded as the lowest-priority layer when given).
            default_filename: Config filename (default "application.yaml").
            extra_paths: Additional configuration files to load.
            skip_standard_paths: Skip standard locations (useful for testing).
        """
        self._model_class = model_class
        self._app_name = app_name
        self._defaults_path = Path(defaults_path) if defaults_path is not None else None
        self._filename = default_filename or self.DEFAULT_CONFIG_FILENAME
        self._layered = LayeredConfig()
        self._model: BaseModel | None = None
        self._validation_errors: list[str] = []

        if not skip_standard_paths:
            self._load_standard_layers()

        if extra_paths:
            for i, path in enumerate(extra_paths):
                self._layered.add_layer(
                    ConfigLayer.from_file(
                        Path(path),
                        name=f"extra_{i}",
                        priority=ConfigPriority.USER + 1 + i,
                    )
                )

        # Add session layer (mutable, runtime-only)
        self._layered.add_layer(
            ConfigLayer(
                name="session",
                priority=ConfigPriority.SESSION,
                mutable=True,
            )
        )

        # Initialize model
        self._rebuild_model()

    def _load_standard_layers(self) -> None:
        """Load configuration from standard locations."""
        # 1. Bundled defaults
        if self._defaults_path is not None:
            self._layered.add_layer(
                ConfigLayer.from_file(
                    self._defaults_path / self._filename,
                    name="defaults",
                    priority=ConfigPriority.DEFAULTS,
                )
            )

        # 2. Global config
        global_path = _global_config_dir(self._app_name) / self._filename
        self._layered.add_layer(
            ConfigLayer.from_file(global_path, name="global", priority=ConfigPriority.GLOBAL)
        )

        # 3. User config
        user_path = _user_config_dir(self._app_name) / self._filename
        self._layered.add_layer(
            ConfigLayer.from_file(
                user_path, name="user", priority=ConfigPriority.USER, mutable=True
            )
        )

    def _rebuild_model(self) -> None:
        """Rebuild the pydantic model from current configuration."""
        data = self._layered.as_dict()
        self._validation_errors.clear()

        try:
            self._model = self._model_class.model_validate(data)
        except ValidationError as e:
            self._validation_errors = [str(err) for err in e.errors()]
            logger.warning("Configuration validation errors: {}", self._validation_errors)
            # Fall back to defaults
            self._model = self._model_class()

    @property
    def model(self) -> BaseModel:
        """Get the validated configuration model."""
        if self._model is None:
            self._rebuild_model()
        return self._model  # type: ignore[return-value]

    def get_user_config_path(self) -> Path:
        """Get the path to the user configuration file."""
        return _user_config_dir(self._app_name) / self._filename

    def ensure_user_config_dir(self) -> Path:
        """Ensure user config directory exists and return its path."""
        path = _user_config_dir(self._app_name)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def validation_errors(self) -> list[str]:
        """Get any validation errors from the last model rebuild."""
        return list(self._validation_errors)

    @property
    def layers(self) -> LayeredConfig:
        """Access the underlying LayeredConfig."""
        return self._layered

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.

        Args:
            key: Dot-separated key path (e.g., "ui.theme").
            default: Default value if key not found.

        Returns:
            The configuration value.
        """
        return self._layered.get(key, default)

    def set(self, key: str, value: Any, *, persist: bool = False) -> None:
        """
        Set a configuration value.

        By default, sets in the session layer (runtime-only).
        Use persist=True to save to the user configuration file.

        Args:
            key: Dot-separated key path.
            value: Value to set.
            persist: If True, save to user config file.
        """
        if persist:
            self._layered.set(key, value, layer_name="user")
            user_layer = self._layered.get_layer("user")
            if user_layer:
                user_layer.save()
        else:
            self._layered.set(key, value, layer_name="session")

        self._model = None  # Invalidate cached model

    def reload(self) -> None:
        """Reload configuration from all sources."""
        # Re-load file-based layers
        for layer in self._layered.layers():
            if isinstance(layer.source, Path) and layer.source.exists():
                reloaded = ConfigLayer.from_file(
                    layer.source,
                    name=layer.name,
                    priority=layer.priority,
                    mutable=layer.mutable,
                )
                self._layered.add_layer(reloaded)

        self._rebuild_model()
        logger.info("Configuration reloaded")

    def save_user_config(self) -> None:
        """Save user layer to file."""
        user_layer = self._layered.get_layer("user")
        if user_layer:
            user_layer.save()
            logger.info("User configuration saved")

    def as_dict(self) -> dict[str, Any]:
        """Return the merged configuration as a dictionary."""
        return self._layered.as_dict()

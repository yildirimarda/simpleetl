"""
Plugin system for SimpleETL extensibility.

Provides a plugin registry, base plugin classes, and format plugin support
with entry_points-based external plugin discovery.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Type

from ..formats.base import DataReader, DataWriter

logger = logging.getLogger(__name__)


class Plugin(ABC):
    """Base class for all plugins.

    Subclasses must provide a ``name`` and ``version``, and may override
    ``setup()`` to perform initialisation when the plugin is registered.
    """

    name: str = ""
    version: str = "0.1.0"

    @abstractmethod
    def setup(self) -> None:
        """Initialise the plugin after registration."""
        pass


class FormatPlugin(Plugin):
    """Base class for format plugins that register custom readers/writers.

    Subclasses must implement ``get_reader()``, ``get_writer()``, and
    ``get_extensions()`` to provide the format implementation.
    """

    @abstractmethod
    def get_reader(self) -> Type[DataReader]:
        """Return the DataReader class for this format."""
        pass

    @abstractmethod
    def get_writer(self) -> Type[DataWriter]:
        """Return the DataWriter class for this format."""
        pass

    @abstractmethod
    def get_extensions(self) -> List[str]:
        """Return the list of file extensions this format handles."""
        pass


class PluginRegistry:
    """Global plugin registry.

    Stores plugins by name and provides lookup, listing, and
    entry-points-based discovery.
    """

    _instance: Optional["PluginRegistry"] = None
    _plugins: Dict[str, Plugin] = {}
    _format_extensions: Dict[str, FormatPlugin] = {}

    def __new__(cls) -> "PluginRegistry":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._plugins = {}
            cls._instance._format_extensions = {}
        return cls._instance

    def register(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: The plugin instance to register.

        Raises:
            ValueError: If a plugin with the same name is already registered.
        """
        if plugin.name in self._plugins:
            raise ValueError(
                f"Plugin '{plugin.name}' is already registered."
            )
        self._plugins[plugin.name] = plugin
        logger.info("Registered plugin: %s v%s", plugin.name, plugin.version)

        # If it is a FormatPlugin, register its extensions.
        if isinstance(plugin, FormatPlugin):
            for ext in plugin.get_extensions():
                ext_lower = ext.lower()
                if ext_lower in self._format_extensions:
                    logger.warning(
                        "Extension '%s' already registered by plugin '%s'. "
                        "Overriding with plugin '%s'.",
                        ext_lower,
                        self._format_extensions[ext_lower].name,
                        plugin.name,
                    )
                self._format_extensions[ext_lower] = plugin
                logger.info(
                    "Registered format extension '%s' from plugin '%s'",
                    ext_lower,
                    plugin.name,
                )

        plugin.setup()

    def get(self, name: str) -> Optional[Plugin]:
        """Retrieve a plugin by name.

        Args:
            name: The plugin name.

        Returns:
            The plugin instance, or None if not found.
        """
        return self._plugins.get(name)

    def get_format_for_extension(self, extension: str) -> Optional[FormatPlugin]:
        """Retrieve the FormatPlugin registered for a given extension.

        Args:
            extension: File extension (e.g. '.csv').

        Returns:
            The FormatPlugin instance, or None if not found.
        """
        return self._format_extensions.get(extension.lower())

    def list_plugins(self) -> List[str]:
        """Return a list of all registered plugin names."""
        return list(self._plugins.keys())

    def list_format_extensions(self) -> List[str]:
        """Return a list of all registered format extensions."""
        return list(self._format_extensions.keys())

    def discover_entry_points(self, group: str = "simpleetl.formats") -> int:
        """Discover and register plugins via setuptools entry points.

        Args:
            group: The entry point group to scan.

        Returns:
            The number of plugins discovered and registered.
        """
        count = 0
        try:
            from importlib.metadata import entry_points
        except ImportError:
            # Python < 3.9 fallback
            try:
                from importlib_metadata import entry_points  # type: ignore[no-redef]
            except ImportError:
                logger.warning(
                    "importlib.metadata not available; "
                    "entry point discovery skipped."
                )
                return 0

        try:
            # importlib.metadata.entry_points() API changed in 3.12
            eps = entry_points()
            if hasattr(eps, "select"):
                # Python 3.12+ / importlib_metadata >= 5.0
                group_eps = eps.select(group=group)
            else:
                # Python 3.9-3.11
                group_eps = eps.get(group, [])  # type: ignore[attr-defined]
        except Exception as exc:
            logger.warning("Failed to load entry points for group '%s': %s", group, exc)
            return 0

        for ep in group_eps:
            try:
                plugin_cls = ep.load()
                if isinstance(plugin_cls, type) and issubclass(plugin_cls, Plugin):
                    plugin = plugin_cls()
                elif isinstance(plugin_cls, Plugin):
                    plugin = plugin_cls
                else:
                    logger.warning(
                        "Entry point '%s' is not a Plugin instance or class; skipping.",
                        ep.name,
                    )
                    continue
                self.register(plugin)
                count += 1
            except Exception as exc:
                logger.warning(
                    "Failed to load plugin from entry point '%s': %s",
                    ep.name,
                    exc,
                )

        return count

    def reset(self) -> None:
        """Clear all registered plugins (primarily for testing)."""
        self._plugins.clear()
        self._format_extensions.clear()


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_registry = PluginRegistry()


def register_plugin(plugin: Plugin) -> None:
    """Register a plugin in the global registry.

    Args:
        plugin: The plugin instance to register.
    """
    _registry.register(plugin)


def get_plugin(name: str) -> Optional[Plugin]:
    """Retrieve a plugin by name from the global registry.

    Args:
        name: The plugin name.

    Returns:
        The plugin instance, or None if not found.
    """
    return _registry.get(name)


def list_plugins() -> List[str]:
    """List all registered plugin names."""
    return _registry.list_plugins()


def register_format(
    extensions: List[str],
    reader_cls: Type[DataReader],
    writer_cls: Type[DataWriter],
    plugin_name: str = "custom-format",
    version: str = "0.1.0",
) -> None:
    """Programmatically register a custom format.

    Creates a FormatPlugin behind the scenes and registers it.

    Args:
        extensions: File extensions handled by this format (e.g. ['.custom']).
        reader_cls: DataReader subclass for reading the format.
        writer_cls: DataWriter subclass for writing the format.
        plugin_name: Name for the auto-generated format plugin.
        version: Version string for the auto-generated format plugin.
    """

    _name = plugin_name
    _version = version

    class _AutoFormatPlugin(FormatPlugin):
        name = _name
        version = _version

        def setup(self) -> None:
            logger.debug("Format plugin '%s' setup complete.", self.name)

        def get_reader(self) -> Type[DataReader]:
            return reader_cls

        def get_writer(self) -> Type[DataWriter]:
            return writer_cls

        def get_extensions(self) -> List[str]:
            return list(extensions)

    register_plugin(_AutoFormatPlugin())


def get_format_registry() -> PluginRegistry:
    """Return the global PluginRegistry instance."""
    return _registry

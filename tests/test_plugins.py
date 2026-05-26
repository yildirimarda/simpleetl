"""
Tests for the plugin system (plugins.py).

Covers: Plugin, FormatPlugin, PluginRegistry (singleton, register, get,
list, reset, discover_entry_points), module-level convenience functions,
register_format(), and FormatPlugin extension override warning.
"""

import logging
import pytest
from typing import List, Type
from unittest.mock import patch, MagicMock

from simpleetl.core.plugins import (
    Plugin,
    FormatPlugin,
    PluginRegistry,
    register_plugin,
    get_plugin,
    list_plugins,
    register_format,
    get_format_registry,
)
from simpleetl.formats.base import DataReader, DataWriter
import pandas as pd


# ---------------------------------------------------------------------------
# Concrete test implementations
# ---------------------------------------------------------------------------

class DummyReader(DataReader):
    """Minimal DataReader for testing."""

    def read(self, source, **kwargs):
        return pd.DataFrame()


class DummyWriter(DataWriter):
    """Minimal DataWriter for testing."""

    def write(self, data, destination, **kwargs):
        pass


class ConcretePlugin(Plugin):
    """Concrete Plugin for testing."""

    name = "test-plugin"
    version = "1.0.0"

    def setup(self) -> None:
        self.initialized = True


class AnotherPlugin(Plugin):
    """Another concrete Plugin for testing."""

    name = "another-plugin"
    version = "2.0.0"

    def setup(self) -> None:
        self.initialized = True


class ConcreteFormatPlugin(FormatPlugin):
    """Concrete FormatPlugin for testing."""

    name = "test-format"
    version = "1.0.0"

    def setup(self) -> None:
        self.initialized = True

    def get_reader(self) -> Type[DataReader]:
        return DummyReader

    def get_writer(self) -> Type[DataWriter]:
        return DummyWriter

    def get_extensions(self) -> List[str]:
        return [".test", ".tst"]


class AnotherFormatPlugin(FormatPlugin):
    """Another concrete FormatPlugin with overlapping extensions."""

    name = "another-format"
    version = "2.0.0"

    def setup(self) -> None:
        self.initialized = True

    def get_reader(self) -> Type[DataReader]:
        return DummyReader

    def get_writer(self) -> Type[DataWriter]:
        return DummyWriter

    def get_extensions(self) -> List[str]:
        return [".tst", ".alt"]


# ---------------------------------------------------------------------------
# Helper to reset singleton between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_registry():
    """Reset the PluginRegistry singleton before and after each test."""
    registry = PluginRegistry()
    registry.reset()
    yield
    registry.reset()


# ---------------------------------------------------------------------------
# Tests for Plugin abstract class
# ---------------------------------------------------------------------------

class TestPluginAbstract:
    def test_cannot_instantiate_directly(self):
        """Plugin is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            Plugin()

    def test_concrete_plugin_can_be_instantiated(self):
        """A concrete subclass with setup() can be instantiated."""
        plugin = ConcretePlugin()
        assert plugin.name == "test-plugin"
        assert plugin.version == "1.0.0"

    def test_default_version(self):
        """Plugin subclasses that do not override version get '0.1.0'."""

        class MinimalPlugin(Plugin):
            name = "minimal"

            def setup(self) -> None:
                pass

        plugin = MinimalPlugin()
        assert plugin.version == "0.1.0"

    def test_setup_is_called_on_register(self):
        """setup() is called when the plugin is registered."""
        plugin = ConcretePlugin()
        assert not hasattr(plugin, "initialized") or not plugin.initialized
        registry = PluginRegistry()
        registry.register(plugin)
        assert plugin.initialized is True


# ---------------------------------------------------------------------------
# Tests for FormatPlugin abstract class
# ---------------------------------------------------------------------------

class TestFormatPluginAbstract:
    def test_cannot_instantiate_directly(self):
        """FormatPlugin is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            FormatPlugin()

    def test_concrete_format_plugin(self):
        """A concrete FormatPlugin subclass can be instantiated."""
        plugin = ConcreteFormatPlugin()
        assert plugin.name == "test-format"
        assert plugin.get_reader() is DummyReader
        assert plugin.get_writer() is DummyWriter
        assert plugin.get_extensions() == [".test", ".tst"]


# ---------------------------------------------------------------------------
# Tests for PluginRegistry singleton
# ---------------------------------------------------------------------------

class TestPluginRegistrySingleton:
    def test_singleton_identity(self):
        """Multiple calls to PluginRegistry() return the same instance."""
        reg1 = PluginRegistry()
        reg2 = PluginRegistry()
        assert reg1 is reg2

    def test_singleton_shares_state(self):
        """State changes in one reference are visible in another."""
        reg1 = PluginRegistry()
        plugin = ConcretePlugin()
        reg1.register(plugin)
        reg2 = PluginRegistry()
        assert "test-plugin" in reg2.list_plugins()

    def test_reset_clears_singleton_state(self):
        """reset() clears plugins and format extensions."""
        registry = PluginRegistry()
        registry.register(ConcretePlugin())
        registry.register(ConcreteFormatPlugin())
        assert len(registry.list_plugins()) > 0
        registry.reset()
        assert registry.list_plugins() == []
        assert registry.list_format_extensions() == []


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.register()
# ---------------------------------------------------------------------------

class TestPluginRegistryRegister:
    def test_register_plugin(self):
        """Registering a plugin stores it in the registry."""
        registry = PluginRegistry()
        plugin = ConcretePlugin()
        registry.register(plugin)
        assert "test-plugin" in registry.list_plugins()

    def test_register_duplicate_raises_value_error(self):
        """Registering a plugin with a duplicate name raises ValueError."""
        registry = PluginRegistry()
        registry.register(ConcretePlugin())
        with pytest.raises(ValueError, match="already registered"):
            registry.register(ConcretePlugin())

    def test_register_format_plugin_also_registers_extensions(self):
        """Registering a FormatPlugin also registers its extensions."""
        registry = PluginRegistry()
        registry.register(ConcreteFormatPlugin())
        assert ".test" in registry.list_format_extensions()
        assert ".tst" in registry.list_format_extensions()

    def test_register_format_plugin_extension_override_warning(self, caplog):
        """When two FormatPlugins share an extension, a warning is logged."""
        registry = PluginRegistry()
        registry.register(ConcreteFormatPlugin())
        with caplog.at_level(logging.WARNING):
            registry.register(AnotherFormatPlugin())
        assert "already registered" in caplog.text
        assert ".tst" in caplog.text

    def test_register_calls_setup(self):
        """register() calls setup() on the plugin."""
        registry = PluginRegistry()
        plugin = ConcretePlugin()
        registry.register(plugin)
        assert plugin.initialized is True


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.get()
# ---------------------------------------------------------------------------

class TestPluginRegistryGet:
    def test_get_existing_plugin(self):
        """get() returns the plugin instance for a registered name."""
        registry = PluginRegistry()
        plugin = ConcretePlugin()
        registry.register(plugin)
        result = registry.get("test-plugin")
        assert result is plugin

    def test_get_nonexistent_plugin(self):
        """get() returns None for a name that is not registered."""
        registry = PluginRegistry()
        assert registry.get("nonexistent") is None


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.get_format_for_extension()
# ---------------------------------------------------------------------------

class TestPluginRegistryGetFormatForExtension:
    def test_with_leading_dot(self):
        """get_format_for_extension() works with a leading dot."""
        registry = PluginRegistry()
        plugin = ConcreteFormatPlugin()
        registry.register(plugin)
        result = registry.get_format_for_extension(".test")
        assert result is plugin

    def test_case_insensitive(self):
        """get_format_for_extension() is case-insensitive."""
        registry = PluginRegistry()
        plugin = ConcreteFormatPlugin()
        registry.register(plugin)
        result = registry.get_format_for_extension(".TEST")
        assert result is plugin

    def test_nonexistent_extension(self):
        """get_format_for_extension() returns None for unknown extensions."""
        registry = PluginRegistry()
        assert registry.get_format_for_extension(".xyz") is None


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.list_plugins()
# ---------------------------------------------------------------------------

class TestPluginRegistryListPlugins:
    def test_empty_list(self):
        """list_plugins() returns an empty list when no plugins are registered."""
        registry = PluginRegistry()
        assert registry.list_plugins() == []

    def test_lists_all_registered_names(self):
        """list_plugins() returns all registered plugin names."""
        registry = PluginRegistry()
        registry.register(ConcretePlugin())
        registry.register(AnotherPlugin())
        names = registry.list_plugins()
        assert "test-plugin" in names
        assert "another-plugin" in names
        assert len(names) == 2


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.list_format_extensions()
# ---------------------------------------------------------------------------

class TestPluginRegistryListFormatExtensions:
    def test_empty_list(self):
        """list_format_extensions() returns an empty list when no format plugins are registered."""
        registry = PluginRegistry()
        assert registry.list_format_extensions() == []

    def test_lists_all_extensions(self):
        """list_format_extensions() returns all registered extensions."""
        registry = PluginRegistry()
        registry.register(ConcreteFormatPlugin())
        extensions = registry.list_format_extensions()
        assert ".test" in extensions
        assert ".tst" in extensions
        assert len(extensions) == 2


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.reset()
# ---------------------------------------------------------------------------

class TestPluginRegistryReset:
    def test_reset_clears_plugins(self):
        """reset() removes all registered plugins."""
        registry = PluginRegistry()
        registry.register(ConcretePlugin())
        registry.reset()
        assert registry.list_plugins() == []

    def test_reset_clears_format_extensions(self):
        """reset() removes all registered format extensions."""
        registry = PluginRegistry()
        registry.register(ConcreteFormatPlugin())
        registry.reset()
        assert registry.list_format_extensions() == []

    def test_reset_allows_reregister(self):
        """After reset(), the same plugin can be registered again."""
        registry = PluginRegistry()
        registry.register(ConcretePlugin())
        registry.reset()
        registry.register(ConcretePlugin())
        assert "test-plugin" in registry.list_plugins()


# ---------------------------------------------------------------------------
# Tests for PluginRegistry.discover_entry_points()
# ---------------------------------------------------------------------------

class TestPluginRegistryDiscoverEntryPoints:
    def test_no_entry_points_returns_zero(self):
        """discover_entry_points() returns 0 when the group has no entry points."""
        registry = PluginRegistry()
        count = registry.discover_entry_points(group="simpleetl.nonexistent.group")
        assert count == 0

    def test_discover_loads_plugins(self, monkeypatch):
        """discover_entry_points() loads and registers plugins from entry points."""
        mock_ep = MagicMock()
        mock_ep.name = "test-entry"
        mock_ep.load.return_value = ConcretePlugin

        mock_eps = MagicMock()
        mock_eps.select.return_value = [mock_ep]

        with patch("importlib.metadata.entry_points", return_value=mock_eps):
            registry = PluginRegistry()
            count = registry.discover_entry_points(group="simpleetl.test")
            assert count == 1
            assert "test-plugin" in registry.list_plugins()

    def test_discover_skips_non_plugin_classes(self, monkeypatch):
        """discover_entry_points() skips entry points that are not Plugin subclasses."""
        mock_ep = MagicMock()
        mock_ep.name = "bad-entry"
        mock_ep.load.return_value = str  # not a Plugin subclass

        mock_eps = MagicMock()
        mock_eps.select.return_value = [mock_ep]

        with patch("importlib.metadata.entry_points", return_value=mock_eps):
            registry = PluginRegistry()
            count = registry.discover_entry_points(group="simpleetl.test")
            assert count == 0

    def test_discover_handles_load_exception(self, monkeypatch, caplog):
        """discover_entry_points() handles exceptions during entry point loading."""
        mock_ep = MagicMock()
        mock_ep.name = "broken-entry"
        mock_ep.load.side_effect = Exception("boom")

        mock_eps = MagicMock()
        mock_eps.select.return_value = [mock_ep]

        with patch("importlib.metadata.entry_points", return_value=mock_eps):
            with caplog.at_level(logging.WARNING):
                registry = PluginRegistry()
                count = registry.discover_entry_points(group="simpleetl.test")
            assert count == 0
            assert "Failed to load" in caplog.text


# ---------------------------------------------------------------------------
# Tests for module-level convenience functions
# ---------------------------------------------------------------------------

class TestModuleLevelFunctions:
    def test_register_plugin(self):
        """register_plugin() registers a plugin in the global registry."""
        plugin = ConcretePlugin()
        register_plugin(plugin)
        assert "test-plugin" in list_plugins()

    def test_get_plugin(self):
        """get_plugin() retrieves a plugin by name from the global registry."""
        register_plugin(ConcretePlugin())
        result = get_plugin("test-plugin")
        assert isinstance(result, ConcretePlugin)

    def test_get_plugin_nonexistent(self):
        """get_plugin() returns None for a nonexistent plugin."""
        result = get_plugin("does-not-exist")
        assert result is None

    def test_list_plugins(self):
        """list_plugins() returns names of all globally registered plugins."""
        register_plugin(ConcretePlugin())
        register_plugin(AnotherPlugin())
        names = list_plugins()
        assert "test-plugin" in names
        assert "another-plugin" in names

    def test_get_format_registry(self):
        """get_format_registry() returns the global PluginRegistry instance."""
        registry = get_format_registry()
        assert isinstance(registry, PluginRegistry)


# ---------------------------------------------------------------------------
# Tests for register_format() convenience function
# ---------------------------------------------------------------------------

class TestRegisterFormat:
    def test_register_format_creates_and_registers_plugin(self):
        """register_format() creates a FormatPlugin and registers it."""
        register_format(
            extensions=[".custom"],
            reader_cls=DummyReader,
            writer_cls=DummyWriter,
            plugin_name="my-custom-format",
        )
        registry = get_format_registry()
        assert "my-custom-format" in registry.list_plugins()
        assert ".custom" in registry.list_format_extensions()

    def test_register_format_multiple_extensions(self):
        """register_format() registers all provided extensions."""
        register_format(
            extensions=[".c1", ".c2", ".c3"],
            reader_cls=DummyReader,
            writer_cls=DummyWriter,
            plugin_name="multi-ext-format",
        )
        registry = get_format_registry()
        assert ".c1" in registry.list_format_extensions()
        assert ".c2" in registry.list_format_extensions()
        assert ".c3" in registry.list_format_extensions()

    def test_register_format_default_name(self):
        """register_format() uses 'custom-format' as the default plugin name."""
        register_format(
            extensions=[".def"],
            reader_cls=DummyReader,
            writer_cls=DummyWriter,
        )
        registry = get_format_registry()
        assert "custom-format" in registry.list_plugins()

    def test_register_format_plugin_is_format_plugin(self):
        """The plugin created by register_format() is a FormatPlugin."""
        register_format(
            extensions=[".fmt"],
            reader_cls=DummyReader,
            writer_cls=DummyWriter,
            plugin_name="fmt-check",
        )
        registry = get_format_registry()
        plugin = registry.get("fmt-check")
        assert isinstance(plugin, FormatPlugin)
        assert plugin.get_reader() is DummyReader
        assert plugin.get_writer() is DummyWriter
        assert plugin.get_extensions() == [".fmt"]

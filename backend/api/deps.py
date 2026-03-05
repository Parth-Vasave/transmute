"""FastAPI dependency injection functions for database connections."""
from functools import lru_cache
from db import FileDB, ConversionDB, ConversionRelationsDB, SettingsDB, DefaultFormatsDB


@lru_cache(maxsize=1)
def _file_db() -> FileDB:
    return FileDB()


@lru_cache(maxsize=1)
def _conversion_db() -> ConversionDB:
    return ConversionDB()


@lru_cache(maxsize=1)
def _conversion_relations_db() -> ConversionRelationsDB:
    return ConversionRelationsDB()


@lru_cache(maxsize=1)
def _settings_db() -> SettingsDB:
    return SettingsDB()


def get_file_db() -> FileDB:
    """Dependency that provides a shared FileDB instance."""
    return _file_db()


def get_conversion_db() -> ConversionDB:
    """Dependency that provides a shared ConversionDB instance."""
    return _conversion_db()


def get_conversion_relations_db() -> ConversionRelationsDB:
    """Dependency that provides a shared ConversionRelationsDB instance."""
    return _conversion_relations_db()


def get_settings_db() -> SettingsDB:
    """Dependency that provides a shared SettingsDB instance."""
    return _settings_db()


@lru_cache(maxsize=1)
def _default_formats_db() -> DefaultFormatsDB:
    return DefaultFormatsDB()


def get_default_formats_db() -> DefaultFormatsDB:
    """Dependency that provides a shared DefaultFormatsDB instance."""
    return _default_formats_db()

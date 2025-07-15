"""Shim module for backward compatibility.
Provides the same API as watchlist_toolkit.data_prep.sqlite_utils
so that legacy import statements like `import sqlite_utils` still work
without path hacks.
"""

from importlib import import_module

_sqlite = import_module("watchlist_toolkit.data_prep.sqlite_utils")

__all__ = getattr(_sqlite, "__all__", [name for name in dir(_sqlite) if not name.startswith("_")])

globals().update({name: getattr(_sqlite, name) for name in __all__}) 
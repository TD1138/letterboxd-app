"""Shim module forwarding to watchlist_toolkit.data_prep.letterboxd_utils"""
from importlib import import_module

_module = import_module("watchlist_toolkit.data_prep.letterboxd_utils")
__all__ = getattr(_module, "__all__", [name for name in dir(_module) if not name.startswith("_")])

globals().update({name: getattr(_module, name) for name in __all__}) 
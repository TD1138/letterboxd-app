"""Top-level package for watchlist_toolkit.

It also provides **compatibility shims** so that legacy imports like

    from sqlite_utils import select_statement_to_df

or

    import letterboxd_utils

continue to work after the project was repackaged.  We simply register the
internal modules under those old names in ``sys.modules``.
"""

from importlib import import_module
import sys

# Compatibility alias: ``sqlite_utils`` -> ``watchlist_toolkit.data_prep.sqlite_utils``
if "sqlite_utils" not in sys.modules:
    sys.modules["sqlite_utils"] = import_module("watchlist_toolkit.data_prep.sqlite_utils")

# Compatibility alias: ``letterboxd_utils`` -> ``watchlist_toolkit.data_prep.letterboxd_utils``
if "letterboxd_utils" not in sys.modules:
    sys.modules["letterboxd_utils"] = import_module("watchlist_toolkit.data_prep.letterboxd_utils")

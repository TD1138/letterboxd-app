from pathlib import Path

_BASE_DIR = Path(__file__).resolve().parents[2]  # project root
_SQL_DIR = _BASE_DIR / "sql"

def read_sql(name: str) -> str:
    """Return the text of `sql/<name>.sql`.

    Parameters
    ----------
    name : str
        Base filename without extension, e.g. ``'all_features_query'``.
    """
    path = _SQL_DIR / f"{name}.sql"
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8") 
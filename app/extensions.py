# app/extensions.py
import duckdb
from flask import current_app

_db_con = None

def get_db():
    global _db_con
    if _db_con is None:
        path = current_app.config["DUCKDB_PATH"]
        _db_con = duckdb.connect(database=path, read_only=False)
        # Optional: nice defaults
        _db_con.execute("PRAGMA enable_object_cache")
        _db_con.execute("PRAGMA threads=8")
    return _db_con

def init_extensions(app):
    # DuckDB is lazy — nothing to do here
    # But you can add Redis, Cache, etc. later
    pass

def table_exists(table_name: str) -> bool:
    """
    Fast, reliable, idiomatic DuckDB table existence check.
    Uses the official system catalog — this is the recommended way.
    """
    return (
        get_db().table(table_name).fetchone()
        is not None
    )
"""
Pool size limits shared by psycopg2 (db.py) and asyncpg (db_async.py).

Neon and other serverless Postgres tiers often allow only a handful of concurrent
connections. Defaults are conservative; override with DB_POOL_MIN / DB_POOL_MAX.
"""
from __future__ import annotations

import os


def pool_min_max() -> tuple[int, int]:
    """Return (min, max) connection counts for pools."""
    try:
        mn = int(os.getenv("DB_POOL_MIN", "2"))
        mx = int(os.getenv("DB_POOL_MAX", "10"))
    except ValueError:
        mn, mx = 2, 10
    mn = max(1, mn)
    mx = max(mn, mx)
    return mn, mx

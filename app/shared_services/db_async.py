from __future__ import annotations

import os
import asyncio
import asyncpg

from .logger_setup import setup_logger
from .db_pool_config import pool_min_max

logger = setup_logger()

_pool = None
_pool_lock = asyncio.Lock()


async def init_async_pool(min_size: int | None = None, max_size: int | None = None) -> asyncpg.Pool:
    """Initialize a global asyncpg pool (same DB_POOL_* limits as psycopg2 pool)."""
    global _pool
    env_min, env_max = pool_min_max()
    if min_size is None:
        min_size = env_min
    if max_size is None:
        max_size = env_max
    async with _pool_lock:
        if _pool is None:
            db_host = os.getenv("PGHOST", "localhost")
            db_password = os.getenv("PGPASSWORD", "xpchex_password")
            db_port = os.getenv("PGPORT", "5432")
            db_name = os.getenv("PGDATABASE", "xpchex")
            db_user = os.getenv("PGUSER", "xpchex_user")
            db_ssl_mode = os.getenv("DB_SSL_MODE", "disable")

            if not all([db_host, db_password, db_user]):
                raise ValueError("Missing required database credentials in environment variables")

            ssl_enabled = db_ssl_mode != "disable"

            _pool = await asyncpg.create_pool(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                database=db_name,
                min_size=min_size,
                max_size=max_size,
                ssl=ssl_enabled,
            )
            logger.info(f"Async pool initialized: min={min_size}, max={max_size} for {db_name}@{db_host}:{db_port}")
    return _pool


async def get_async_pool() -> asyncpg.Pool:
    """Get an initialized async pool (init if needed)."""
    if _pool is None:
        await init_async_pool()
    return _pool


async def close_async_pool():
    """Close the async pool."""
    global _pool
    async with _pool_lock:
        if _pool:
            await _pool.close()
            _pool = None
            logger.info("Async pool closed")




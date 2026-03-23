import os
import threading
from contextlib import contextmanager

from typing import List, Dict, Any, Optional, TypedDict, Union
from dotenv import load_dotenv

import psycopg2
import psycopg2.pool
import requests

from psycopg2.extras import Json, RealDictCursor

import numpy as np

from .logger_setup import setup_logger
from .db_pool_config import pool_min_max

# Load environment variables
load_dotenv()

logger = setup_logger()


def _env_flag(name: str, default: str = "false") -> bool:
    """Return True/False for common truthy strings."""
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


# Toggle pooling from env; default ON for better performance with concurrency
USE_CONNECTION_POOLING = _env_flag("DB_USE_POOL", "true")

# Connection pool (thread-safe)
_connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()

# Thread-local storage for connection reuse within workers
_thread_local = threading.local()

# postgres connection without pooling


def init_connection_pool(minconn: Optional[int] = None, maxconn: Optional[int] = None):
    """
    Initialize connection pool for PostgreSQL database.

    Defaults come from DB_POOL_MIN / DB_POOL_MAX (see db_pool_config), tuned for
    Neon and small Postgres tiers. Override env or pass explicit minconn/maxconn.
    """
    global _connection_pool

    env_min, env_max = pool_min_max()
    if minconn is None:
        minconn = env_min
    if maxconn is None:
        maxconn = env_max

    with _pool_lock:
        if _connection_pool is None:
            # Get database connection details from environment variables
            db_host = os.getenv("PGHOST", "localhost")
            db_password = os.getenv("PGPASSWORD", "xpchex_password")
            db_port = os.getenv("PGPORT", "5432")
            db_name = os.getenv("PGDATABASE", "xpchex")
            db_user = os.getenv("PGUSER", "xpchex_user")
            db_ssl_mode = os.getenv("DB_SSL_MODE", "disable")
            
            if not all([db_host, db_password, db_user]):
                error_msg = "Missing required database credentials in environment variables"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            try:
                _connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    host=db_host,
                    database=db_name,
                    user=db_user,
                    password=db_password,
                    port=db_port,
                    sslmode=db_ssl_mode,
                    # Add keepalive settings to prevent connection drops
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                    # Add connection timeout
                    connect_timeout=10
                )
                logger.info(f"Connection pool initialized with keepalives: min={minconn}, max={maxconn} for {db_name}@{db_host}:{db_port}")
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
    
    return _connection_pool


def validate_connection(conn):
    """
    Validate that a connection is still alive and working.
    Returns True if connection is valid, False otherwise.
    """
    try:
        # Try to execute a simple query
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        return True
    except Exception as e:
        logger.warning(f"Connection validation failed: {e}")
        return False


def get_postgres_connection(
    table_name: str = None,
    use_pool: Optional[bool] = None,
    reuse_thread_connection: bool = False
):
    """
    Get a PostgreSQL database connection.
    
    Supports two modes:
    1. Connection pooling (default): Gets connection from pool
    2. Direct connection: Creates new connection (for backward compatibility)
    
    With reuse_thread_connection=True, returns the same connection for the current thread
    (useful for worker threads that make multiple DB calls).
    
    :param table_name: Optional. Name of the table to interact with (not used currently)
    :param use_pool: If True, use connection pool. None -> env default.
    :param reuse_thread_connection: If True, reuse connection within current thread (default: False)
    :return: Connection object
    """
    # Decide pooling mode once for this call
    effective_use_pool = USE_CONNECTION_POOLING if use_pool is None else use_pool

    # If reuse is requested, check thread-local storage first
    if reuse_thread_connection:
        if hasattr(_thread_local, 'connection'):
            conn = _thread_local.connection
            # Validate the connection before returning
            if validate_connection(conn):
                return conn
            else:
                # Connection is stale, remove it and get a new one
                logger.warning("Thread-local connection is stale, getting new one")
                delattr(_thread_local, 'connection')
    
    # Get connection from pool or create new one
    if effective_use_pool:
        if _connection_pool is None:
            init_connection_pool()
        
        try:
            # Try up to 3 times to get a valid connection from pool
            max_retries = 3
            for attempt in range(max_retries):
                conn = _connection_pool.getconn()
                if conn:
                    # Validate connection before returning
                    if validate_connection(conn):
                        # Store in thread-local if reuse is requested
                        if reuse_thread_connection:
                            _thread_local.connection = conn
                        return conn
                    else:
                        logger.warning(f"Got stale connection from pool (attempt {attempt + 1}/{max_retries}), discarding")
                        try:
                            # Important: always return checked-out pooled connections via putconn.
                            # If we only close() here, pool bookkeeping can leak a used slot.
                            _connection_pool.putconn(conn, close=True)
                        except Exception as put_err:
                            logger.warning(f"Failed to discard stale pooled connection cleanly: {put_err}")
                            try:
                                conn.close()
                            except Exception:
                                pass
                        # Continue to next attempt
                else:
                    logger.warning(f"Failed to get connection from pool (attempt {attempt + 1}/{max_retries})")
            
            # If all pool attempts failed, create a fresh connection as fallback
            logger.warning("All pooled connections failed validation, creating fresh connection")
            conn = psycopg2.connect(
                host=os.getenv("PGHOST", "localhost"),
                database=os.getenv("PGDATABASE", "xpchex"),
                user=os.getenv("PGUSER", "xpchex_user"),
                password=os.getenv("PGPASSWORD", "xpchex_password"),
                port=os.getenv("PGPORT", "5432"),
                sslmode=os.getenv("DB_SSL_MODE", "disable"),
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
                connect_timeout=10
            )
            # Don't put this fresh connection back in the pool
            logger.info("Created fresh connection outside of pool")
            return conn
            
        except Exception as e:
            logger.error(f"Error getting connection: {e}")
            raise
    else:
        # Fallback to direct connection (non-pooled)
        db_host = os.getenv("PGHOST", "localhost")
        db_password = os.getenv("PGPASSWORD", "xpchex_password")
        db_port = os.getenv("PGPORT", "5432")
        db_name = os.getenv("PGDATABASE", "xpchex")
        db_user = os.getenv("PGUSER", "xpchex_user")
        db_ssl_mode = os.getenv("DB_SSL_MODE", "disable")

        if not all([db_host, db_password, db_user]):
            error_msg = "Missing required database credentials in environment variables"
            logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            conn = psycopg2.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password,
                port=db_port,
                sslmode=db_ssl_mode,
                # Add keepalive settings to prevent connection drops
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
                # Add connection timeout
                connect_timeout=10
            )
            logger.info(f"Successfully connected to database (non-pooled) with keepalives: {db_name} as user {db_user} at {db_host}:{db_port}")
            return conn
        except psycopg2.OperationalError as e:
            logger.error(f"Unable to connect to database. Error: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while connecting to database: {e}")
            raise


def release_thread_connection():
    """
    Release the thread-local connection back to the pool.
    Call this when a worker thread finishes processing.
    """
    if hasattr(_thread_local, 'connection'):
        conn = _thread_local.connection
        if _connection_pool and conn:
            try:
                _connection_pool.putconn(conn)
            except Exception as e:
                logger.error(f"Error returning connection to pool: {e}")
        delattr(_thread_local, 'connection')


def release_connection(conn):
    """
    Return a pooled connection back to the pool.
    If the connection is closed or unhealthy (e.g. server dropped mid-session),
    pass close=True so the pool does not hand it out again.
    Safe to call in finally blocks.
    """
    if not conn or not _connection_pool:
        return
    discard = bool(getattr(conn, "closed", False))
    if not discard:
        try:
            discard = not validate_connection(conn)
        except Exception:
            discard = True
    if discard:
        logger.warning("Releasing pooled connection with close=True (stale or dead)")
    try:
        _connection_pool.putconn(conn, close=discard)
    except Exception as e:
        logger.error(f"Error returning connection to pool: {e}")


@contextmanager
def pooled_connection(reuse_thread_connection: bool = False, use_pool: Optional[bool] = None):
    """
    Context manager that returns a pooled connection and ensures it is
    returned to the pool on exit.
    """
    effective_use_pool = USE_CONNECTION_POOLING if use_pool is None else use_pool

    # Shortcut to non-pooled connections when pooling is disabled
    if not effective_use_pool:
        with non_pooled_connection() as conn:
            yield conn
        return

    conn = get_postgres_connection(use_pool=True, reuse_thread_connection=reuse_thread_connection)
    try:
        yield conn
    finally:
        # If using thread reuse, delegate to release_thread_connection
        if reuse_thread_connection:
            release_thread_connection()
        else:
            release_connection(conn)


@contextmanager
def non_pooled_connection():
    """
    Context manager that creates a new, non-pooled connection and ensures it is
    closed on exit.
    """
    conn = None
    try:
        # Get a new, non-pooled connection
        conn = get_postgres_connection(use_pool=False)
        yield conn
    except Exception as e:
        logger.error(f"Error within non_pooled_connection context: {e}")
        raise
    finally:
        # Close the connection if it was successfully established
        if conn:
            try:
                conn.close()
                logger.debug("Closed non-pooled connection")
            except Exception as e:
                logger.error(f"Error closing non-pooled connection: {e}")


def close_connection_pool():
    """
    Close all connections in the pool.
    Call this when shutting down the application.
    """
    global _connection_pool
    
    with _pool_lock:
        if _connection_pool:
            try:
                _connection_pool.closeall()
                logger.info("Connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}")
            finally:
                _connection_pool = None
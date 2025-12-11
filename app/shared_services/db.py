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

# Load environment variables
load_dotenv()

logger = setup_logger()

# Connection pool (thread-safe)
_connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()

# Thread-local storage for connection reuse within workers
_thread_local = threading.local()


def init_connection_pool(minconn: int = 5, maxconn: int = 50):
    """
    Initialize connection pool for PostgreSQL database.
    
    :param minconn: Minimum number of connections in pool (default: 5)
    :param maxconn: Maximum number of connections in pool (default: 50)
    :return: Connection pool object
    """
    global _connection_pool
    
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
                    sslmode=db_ssl_mode
                )
                logger.info(f"Connection pool initialized: min={minconn}, max={maxconn} for {db_name}@{db_host}:{db_port}")
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
    
    return _connection_pool


def get_postgres_connection(table_name: str = None, use_pool: bool = True, reuse_thread_connection: bool = False):
    """
    Get a PostgreSQL database connection.
    
    Supports two modes:
    1. Connection pooling (default): Gets connection from pool
    2. Direct connection: Creates new connection (for backward compatibility)
    
    With reuse_thread_connection=True, returns the same connection for the current thread
    (useful for worker threads that make multiple DB calls).
    
    :param table_name: Optional. Name of the table to interact with (not used currently)
    :param use_pool: If True, use connection pool (default: True)
    :param reuse_thread_connection: If True, reuse connection within current thread (default: False)
    :return: Connection object
    """
    # If reuse is requested, check thread-local storage first
    if reuse_thread_connection:
        if hasattr(_thread_local, 'connection'):
            return _thread_local.connection
    
    # Get connection from pool or create new one
    if use_pool:
        if _connection_pool is None:
            init_connection_pool()
        
        try:
            conn = _connection_pool.getconn()
            if conn:
                # Store in thread-local if reuse is requested
                if reuse_thread_connection:
                    _thread_local.connection = conn
                return conn
            else:
                raise Exception("Failed to get connection from pool")
        except Exception as e:
            logger.error(f"Error getting connection from pool: {e}")
            raise
    else:
        # Fallback to direct connection (backward compatibility)
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
                sslmode=db_ssl_mode
            )
            logger.info(f"Successfully connected to database: {db_name} as user {db_user} at {db_host}:{db_port}")
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
    Safe to call in finally blocks.
    """
    if conn and _connection_pool:
        try:
            _connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {e}")


@contextmanager
def pooled_connection(reuse_thread_connection: bool = False):
    """
    Context manager that returns a pooled connection and ensures it is
    returned to the pool on exit.
    """
    conn = get_postgres_connection(use_pool=True, reuse_thread_connection=reuse_thread_connection)
    try:
        yield conn
    finally:
        # If using thread reuse, delegate to release_thread_connection
        if reuse_thread_connection:
            release_thread_connection()
        else:
            release_connection(conn)


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


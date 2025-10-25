import os

from typing import List, Dict, Any, Optional, TypedDict, Union
from dotenv import load_dotenv

import psycopg2
import requests

from psycopg2.extras import Json, RealDictCursor

import numpy as np

from .logger_setup import setup_logger

# Load environment variables
load_dotenv()

logger = setup_logger()


def get_postgres_connection(table_name: str = None):
    """
    Establish and return a connection to the PostgreSQL database using Neon connection.
    For direct database connections, we use the connection string from Neon Dashboard.
    
    :param table_name: Optional. Name of the table to interact with (not used currently)
    :return: Connection object
    """
    # Get Neon connection details from environment variables
    db_host = os.getenv("PGHOST")  # Host from connection string
    db_password = os.getenv("PGPASSWORD")  # Password from connection string
    db_port = os.getenv("PGPORT", "5432")  # Port with default value
    db_name = os.getenv("PGDATABASE", "xpchex")  # Default to 'neondb' for Neon
    db_user = os.getenv("PGUSER")  # User from connection string

    if not all([db_host, db_password, db_user]):
        error_msg = "Missing required database credentials in environment variables"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Direct PostgreSQL connection to Neon database
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
            sslmode='require'  # Neon requires SSL
        )
        logger.info(f"Successfully connected to Neon database: {db_name} as user {db_user}")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Unable to connect to database. Error: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while connecting to Neon: {e}")
        raise


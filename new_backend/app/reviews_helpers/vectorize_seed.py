import os
from typing import List
from openai import OpenAI
import numpy as np
from app.shared_services.db import get_postgres_connection
from app.reviews_helpers.vectorizer import get_embedding
from app.shared_services.logger_setup import setup_logger

logger = setup_logger()

def vectorize_seed():
    vectorize_statement_taxonomy_seed()
    vectorize_canonical_aliases_seed()

def vectorize_statement_taxonomy_seed():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    # Only vectorize the description column
    cursor.execute("SELECT canonical_id, description FROM statement_taxonomy")
    statements = cursor.fetchall()
    for canonical_id, description in statements:
        if description:  # Only vectorize if description exists
            embedding = get_embedding(description)
            if embedding:  # Only update if embedding was successful
                cursor.execute("UPDATE statement_taxonomy SET statement_embedding = %s WHERE canonical_id = %s", (embedding, canonical_id))
                logger.info(f"Vectorized {canonical_id}")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Statement taxonomy seed vectorized")

def vectorize_canonical_aliases_seed():
    conn = get_postgres_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT alias FROM canonical_aliases")
    aliases = cursor.fetchall()
    for (alias,) in aliases:
        embedding = get_embedding(alias)
        if embedding:  # Only update if embedding was successful
            cursor.execute("UPDATE canonical_aliases SET alias_embedding = %s WHERE alias = %s", (embedding, alias))
            logger.info(f"Vectorized alias: {alias}")
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Canonical aliases seed vectorized")

if __name__ == "__main__":
    vectorize_seed()








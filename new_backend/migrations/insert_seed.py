import json
import os
from pathlib import Path
from app.shared_services.db import get_postgres_connection

SEED_DIR = Path(__file__).parent
STATEMENTS_FILE = SEED_DIR / "statement_seed.json"
ALIASES_FILE = SEED_DIR / "alias_seed.json"
ACTIONS_FILE = SEED_DIR / "action_seed.json"

def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def upsert_statements(conn, rows):
    sql = """
    INSERT INTO statement_taxonomy (
      canonical_id, review_section, category, subcategory, subcategory2, display_label, description, examples, source
    ) VALUES (
      %(canonical_id)s, %(review_section)s, %(category)s, %(subcategory)s, %(subcategory2)s, %(display_label)s, %(description)s, %(examples)s, 'SEED'
    )
    ON CONFLICT (canonical_id) DO UPDATE SET
      review_section = %(review_section)s,
      category = %(category)s,
      subcategory = %(subcategory)s,
      subcategory2 = %(subcategory2)s,
      display_label = %(display_label)s,
      description = %(description)s,
      examples = %(examples)s,
      source = 'SEED';
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, {
                "canonical_id": r["canonical_id"],
                "review_section": r["review_section"],
                "category": r["category"],
                "subcategory": r.get("subcategory"),
                "subcategory2": r.get("subcategory2"),
                "display_label": r["display_label"],
                "description": r.get("description"),
                "examples": json.dumps(r.get("examples")) if r.get("examples") is not None else None,
            })
    conn.commit()

def upsert_actions(conn, rows):
    sql = """
    INSERT INTO statement_taxonomy (
      canonical_id, review_section, category, subcategory, subcategory2, display_label, description, examples, source
    ) VALUES (
      %(canonical_id)s, %(review_section)s, %(category)s, %(subcategory)s, %(subcategory2)s, %(display_label)s, %(description)s, %(examples)s, 'SEED'
    )
    ON CONFLICT (canonical_id) DO UPDATE SET
      review_section = %(review_section)s,
      category = %(category)s,
      subcategory = %(subcategory)s,
      subcategory2 = %(subcategory2)s,
      display_label = %(display_label)s,
      description = %(description)s,
      examples = %(examples)s,
      source = 'SEED';
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, {
                "canonical_id": r["canonical_id"],
                "review_section": r["review_section"],
                "category": r["category"],
                "subcategory": r.get("subcategory"),
                "subcategory2": r.get("subcategory2"),
                "display_label": r["display_label"],
                "description": r.get("description"),
                "examples": json.dumps(r.get("examples")) if r.get("examples") is not None else None,
            })
    conn.commit()


def upsert_aliases(conn, rows):
    sql = """
    INSERT INTO canonical_aliases (
      alias, canonical_id, locale, source, confidence, is_active
    ) VALUES (
      %(alias)s, %(canonical_id)s, %(locale)s, %(source)s, %(confidence)s, %(is_active)s
    )
    ON CONFLICT (alias) DO UPDATE SET
      canonical_id = %(canonical_id)s,
      locale = %(locale)s,
      source = %(source)s,
      confidence = %(confidence)s,
      is_active = %(is_active)s;
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, {
                "alias": r["alias"],
                "canonical_id": r["canonical_id"],
                "locale": r.get("locale"),
                "source": "SEED",
                "confidence": r.get("confidence"),
                "is_active": True,
            })
    conn.commit()

def main():
    statements = load_json(STATEMENTS_FILE)
    aliases = load_json(ALIASES_FILE)
    actions = load_json(ACTIONS_FILE)
    conn = get_postgres_connection()
    try:
        upsert_statements(conn, statements)
        upsert_aliases(conn, aliases)
        upsert_actions(conn, actions)
        print("Seed inserted: statements=", len(statements), "aliases=", len(aliases), "actions=", len(actions))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
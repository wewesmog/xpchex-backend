from logging import getLogger

logger = getLogger(__name__)

# Prompt the LLM to choose an existing canonical_id (preferred) or create a new
# snake_case canonical_id following strict guidelines. The caller should pass a
# short list of top similar candidates to keep the context lean.

def canonize_statement(statement: str, existing_statements: list) -> str:
    """Generate a strong, few-shot prompt for canonizing a statement.

    existing_statements: list of objects with .statement and .canonical_id.
    The list should already be filtered to the top-N nearest candidates.
    """
    # Format candidates for display in a compact, aligned form
    formatted_candidates = "\n".join(
        [f"- {i+1}. {s.statement}  =>  {s.canonical_id}" for i, s in enumerate(existing_statements)]
    )

    prompt = f"""
    Role: You map noisy, user-written issue sentences to a stable canonical taxonomy.

    Task: Given a new statement, either:
    - MAP it to one of the provided candidate canonical IDs if the meaning is the same, or
    - CREATE a new concise snake_case canonical_id when no candidate captures the core meaning.

    Input statement:
    "{statement}"

    Candidate canonical mappings (pick one if truly equivalent):
    {formatted_candidates if formatted_candidates else "- (no close candidates provided)"}

    Rules:
    1) Prefer an existing candidate if the meanings are equivalent or near-equivalent.
       - Consider paraphrases, tense/number changes, synonyms (slow/laggy, fail/not working, crash/close, login/sign in).
       - Small specificity differences ("login not working" vs "cannot login after update") should map to the same canonical if the core problem is identical.
    2) Only create NEW IDs when the concept is materially different from all candidates.
    3) Canonical ID format (strict):
       - snake_case, lowercase letters and digits only
       - 2–6 tokens; focus on core noun/verb (e.g., slow_transactions, login_failure, app_crash, balance_not_updating)
       - Avoid app names, brand names, or device specifics
       - Use consistent terms from this mini-taxonomy when applicable: slow_transactions, app_crash, app_freeze, login_failure, otp_not_received, otp_entry_issue, balance_not_updating, app_not_opening, app_update_regression, customer_support_unresponsive, feature_missing, security_concern
    4) If mapping to an existing candidate, return that candidate’s canonical_id exactly.
    5) Output must be ONLY this JSON object (no extra text):
       {{
         "canonical_id": "snake_case_id_or_existing_candidate",
         "reasoning": "Why this mapping or new id is correct in one sentence",
         "error": null
       }}

    A few-shot examples:
    - Input: "App is very slow during transfers"
      → {{"canonical_id": "slow_transactions", "reasoning": "Performance slowness while executing transactions.", "error": null}}
    - Input: "Cannot log in after update"
      → {{"canonical_id": "login_failure", "reasoning": "User unable to authenticate; regression after update does not change the core issue.", "error": null}}
    - Input: "Balances don’t refresh after sending money"
      → {{"canonical_id": "balance_not_updating", "reasoning": "Account balance fails to refresh post-transaction.", "error": null}}
    - Input: "The app keeps crashing"
      → {{"canonical_id": "app_crash", "reasoning": "Repeated crashes describe the same failure mode.", "error": null}}

    Now produce the JSON for the actual input statement.
    """

    logger.debug(f"Generated strengthened canonization prompt for statement: {statement}")
    logger.debug(f"Candidate list size: {len(existing_statements)}")
    return prompt






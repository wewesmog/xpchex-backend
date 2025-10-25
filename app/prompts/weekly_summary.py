def get_overall_summary_prompt(context: dict) -> str:
    return f"""
    Craft a concise 2–3 sentence executive statement. Avoid starting with time phrases.
    Style guidance (pick naturally, do not repeat the same opener each time):
    - "Customers are saying …"
    - "Feedback highlights …"
    - "Users consistently report …"
    Then transition into issues without repeating the opener:
    - "There are failures on …"
    - "Key breakdowns are …"
    - "Primary friction points include …"
    End with one short, actionable priority.

    Use ONLY the arrays below.
    Positive mentions (top items first):
    {format_positive_mentions(context.get('positive_mentions', []))}
    Issues (top items first):
    {format_issues(context.get('issues', []))}

    Context (do not restate dates verbatim): app={context.get('app_id')}, totals: positives={context.get('total_positive', 0)}, issues={context.get('total_issues', 0)}
    Output must be a single paragraph without headings or bullets.
    """


def get_positive_summary_prompt(context: dict) -> str:
    return f"""
    Write a single 1–2 sentence statement. Avoid time phrases.
    Use varied natural openings (choose one):
    - "Customers are saying …"
    - "Feedback highlights …"
    - "Users appreciate …"
    Focus on the clearest positive themes and their impact. No headings/bullets.

    Data:
    {format_positive_mentions(context.get('positive_mentions', []))}
    """


def get_issues_summary_prompt(context: dict) -> str:
    return f"""
    Write a single 1–2 sentence statement. Avoid time phrases.
    Use varied natural openings (choose one):
    - "There are failures on …"
    - "Key breakdowns are …"
    - "Primary friction points include …"
    Highlight the most impactful issues (include severity when available). No headings/bullets.

    Data:
    {format_issues(context.get('issues', []))}
    """


def format_positive_mentions(mentions: list) -> str:
    if not mentions:
        return "- None"
    lines = []
    for m in mentions:
        lines.append(
            f"- {m.get('grouping_id', 'unknown')} (count {m.get('count', 0)}, avg_impact {m.get('avg_impact', 0)})"
        )
    return "\n".join(lines)


def format_issues(issues: list) -> str:
    if not issues:
        return "- None"
    lines = []
    for i in issues:
        lines.append(
            f"- {i.get('grouping_id', 'unknown')} (count {i.get('count', 0)}, severity {i.get('severity', 'n/a')}, avg_impact {i.get('avg_impact', 0)})"
        )
    return "\n".join(lines)



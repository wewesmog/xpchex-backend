"""Centralised Logfire bootstrap.  Call ``configure()`` once per process.

Instruments automatically:
  - OpenAI      → every LLM call (sync + async), including inside LangGraph nodes
  - psycopg2    → SQL queries via pooled / non-pooled connections
  - httpx       → outbound HTTP (scraper, external APIs)

FastAPI instrumentation is handled by the caller (``main.py``) via
``logfire.instrument_fastapi(app)`` so it can receive the ``app`` instance.

LangGraph: no separate plugin needed.  Because OpenAI is instrumented, all
LLM calls inside graph nodes appear as children of the enclosing logfire span.
Wrap ``graph.ainvoke()`` / ``graph.invoke()`` in ``logfire.span(...)`` at the
call site to group a full graph run under one trace.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_configured = False


def configure(service_name: str = "xpchex-backend") -> bool:
    """Configure Logfire for the current process.

    Returns ``True`` when telemetry is active, ``False`` when the token is
    missing or the package is not installed (graceful no-op).
    Safe to call more than once — subsequent calls are skipped.
    """
    global _configured
    if _configured:
        return True

    token = os.getenv("LOGFIRE_TOKEN")
    if not token:
        logger.debug("LOGFIRE_TOKEN not set — Logfire telemetry disabled")
        return False

    try:
        import logfire  # noqa: PLC0415

        logfire.configure(
            token=token,
            service_name=service_name,
        )

        # OpenAI — captures all completions / embeddings (sync + async).
        # This also covers every LLM call made inside LangGraph node functions
        # because they all go through the shared openai / async_openai clients.
        try:
            logfire.instrument_openai()
        except Exception as exc:  # pragma: no cover
            logger.warning("logfire.instrument_openai failed: %s", exc)

        # psycopg / psycopg2 — SQL query tracing (method name varies by logfire version)
        try:
            if hasattr(logfire, "instrument_psycopg2"):
                logfire.instrument_psycopg2()
            elif hasattr(logfire, "instrument_psycopg"):
                logfire.instrument_psycopg()
            else:
                logger.warning(
                    "Logfire has no psycopg instrumentation method (expected instrument_psycopg2 or instrument_psycopg)"
                )
        except Exception as exc:  # pragma: no cover
            logger.warning("logfire psycopg instrumentation failed: %s", exc)

        # httpx — outbound HTTP calls (Google Play scraper, any external APIs)
        try:
            logfire.instrument_httpx()
        except Exception as exc:  # pragma: no cover
            logger.warning("logfire.instrument_httpx failed: %s", exc)

        _configured = True
        logger.info("Logfire configured service_name=%s", service_name)
        return True

    except ImportError:  # pragma: no cover
        logger.warning("logfire package not installed — telemetry disabled")
        return False

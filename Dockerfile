# Python 3.11 (matches local dev); slim keeps image smaller
FROM python:3.11-slim

WORKDIR /app

# System: build tools for psycopg2 / wheels, Postgres headers, curl for HEALTHCHECK
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Layer cache: deps before app code
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Align with main.py GET /health (DB check)
HEALTHCHECK --interval=30s --timeout=10s --start-period=25s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
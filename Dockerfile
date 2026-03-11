# ── Base: Python 3.12 slim + Java (required by PySpark 3.5) ──────────────────
FROM python:3.12-slim

# Install Java (PySpark dependency — Debian trixie ships Java 21, works with PySpark 3.5)
RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

# data/ is mounted as a volume at runtime (missions + datasets live-editable)
# progress/ dir will be created inside the container volume

EXPOSE 8000

ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

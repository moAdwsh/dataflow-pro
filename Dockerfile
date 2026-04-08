# ─────────────────────────────────────────────────────────────────
# DataFlow Pro — Dockerfile
# ─────────────────────────────────────────────────────────────────
# Build:   docker build -t dataflow-pro .
# Run API: docker run -p 8000:8000 dataflow-pro
# Run CLI: docker run -it dataflow-pro python src/main.py --demo
# ─────────────────────────────────────────────────────────────────

# ── Base image ────────────────────────────────────────────────────
FROM python:3.11-slim

# Keeps Python from writing .pyc files and enables stdout flushing
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ── Working directory inside the container ────────────────────────
WORKDIR /app

# ── System dependencies (minimal) ────────────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ───────────────────────────────────────────
# Copy requirements first so Docker can cache this layer
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Application source ────────────────────────────────────────────
COPY src/ ./src/

# ── Expose FastAPI port ───────────────────────────────────────────
EXPOSE 8000

# ── Default command: start the API server ─────────────────────────
# Override at runtime for other modes:
#   docker run -it dataflow-pro python src/main.py --demo
#   docker run -it dataflow-pro python src/main.py --benchmark
CMD ["uvicorn", "src.api_server:app", "--host", "0.0.0.0", "--port", "8000"]

# =============================================================================
# Stage 1: Builder — install Python dependencies
# =============================================================================
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies needed for some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY source_code/requirements.txt .

# Install into a prefix directory so we can copy cleanly to the runtime stage
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# =============================================================================
# Stage 2: Runtime — lean final image
# =============================================================================
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime libs only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY source_code/ .

# Run as non-root user for security
RUN useradd --no-create-home --shell /bin/false appuser \
    && chown -R appuser:appuser /app
USER appuser

# Expose the configurable port (default 8080)
EXPOSE 8080

# Exec-form CMD: uvicorn receives SIGTERM directly (graceful shutdown).
# PORT and LOG_LEVEL defaults are set here; docker-compose overrides via env.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1 --log-level $(echo ${LOG_LEVEL:-info} | tr '[:upper:]' '[:lower:]')"]

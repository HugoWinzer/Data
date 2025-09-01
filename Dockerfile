# file: Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY src ./src

# Non-root for Cloud Run best practices
RUN useradd -m appuser
USER appuser

# Cloud Run expects web server on $PORT (default 8080)
ENV PORT=8080
CMD ["uvicorn", "service.app:app", "--host", "0.0.0.0", "--port", "8080"]

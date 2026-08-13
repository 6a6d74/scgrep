FROM python:3.12-slim

# Prevent Python from writing pyc files and buffering stdout/stderr.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code.
COPY scgrep ./scgrep

# Run as an unprivileged user.
RUN useradd --system --create-home scgrep
USER scgrep

# Prometheus metrics port (see METRICS_PORT).
EXPOSE 8000

ENTRYPOINT ["python", "-m", "scgrep"]

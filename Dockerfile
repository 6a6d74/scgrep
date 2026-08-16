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

# Run as an unprivileged user; give it a writable log directory (used when
# LOG_FILE is set and no volume is mounted over it).
RUN useradd --system --create-home scgrep \
    && mkdir -p /var/log/scgrep \
    && chown scgrep:scgrep /var/log/scgrep
USER scgrep

# Prometheus metrics port (see METRICS_PORT).
EXPOSE 8000

ENTRYPOINT ["python", "-m", "scgrep"]

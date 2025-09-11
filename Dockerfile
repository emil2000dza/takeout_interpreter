FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libssl-dev libffi-dev libpq-dev git \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

WORKDIR /app

# Copy and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ src/
COPY .env .env

# Set PYTHONPATH
ENV PYTHONPATH=/app/src

# Entrypoint
ENTRYPOINT ["python", "-m", "topic_modeling.main"]

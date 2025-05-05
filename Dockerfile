FROM python:3.12-slim

# Install system-level build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      python3-dev \
      libgeos-dev \
      wget && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip & install Python requirements
COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /requirements.txt && \
    rm /requirements.txt




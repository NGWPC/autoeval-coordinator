FROM python:3.12-slim AS dev

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      python3-dev \
      libgeos-dev \
      wget && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# For development, src code should be mounted

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt && \
    rm requirements.txt


FROM dev AS prod

# Snapshot of the current codebase
COPY src/ src/

ENTRYPOINT ["python", "src/main.py"]
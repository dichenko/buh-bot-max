FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        unoconv \
        libreoffice-calc \
        libreoffice-writer \
        fonts-dejavu \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY templates ./templates

RUN mkdir -p /app/obrazec

CMD ["python", "-m", "src.main"]

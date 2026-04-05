FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libreoffice-calc \
        libreoffice-writer \
        fontconfig \
        fonts-dejavu \
        fonts-liberation2 \
        fonts-crosextra-carlito \
        fonts-noto-core \
    && fc-cache -f -v \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY templates ./templates

RUN mkdir -p /app/obrazec

CMD ["python", "-m", "src.main"]

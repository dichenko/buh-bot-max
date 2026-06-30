FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libreoffice-calc \
        libreoffice-writer \
        fontconfig \
        fonts-dejavu \
        fonts-liberation2 \
        fonts-crosextra-carlito \
        fonts-noto-core \
    && fc-cache -f -v \
    && rm -rf /var/lib/apt/lists/*

COPY certs/russian-trusted/ /usr/local/share/ca-certificates/russian-trusted/

RUN update-ca-certificates \
    && { \
         for cert in /usr/local/share/ca-certificates/russian-trusted/*.crt; do \
           cat "$cert"; \
           printf '\n'; \
         done; \
       } > /etc/ssl/certs/russian-trusted-ca-bundle.pem

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY templates ./templates

RUN mkdir -p /app/obrazec

CMD ["python", "-m", "src.main"]

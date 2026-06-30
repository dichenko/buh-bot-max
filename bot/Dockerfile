FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY certs/russian-trusted/ /usr/local/share/ca-certificates/russian-trusted/

RUN update-ca-certificates \
    && { \
         for cert in /usr/local/share/ca-certificates/russian-trusted/*.crt; do \
           cat "$cert"; \
           printf '\n'; \
         done; \
       } > /etc/ssl/certs/russian-trusted-ca-bundle.pem

ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/russian-trusted-ca-bundle.pem

COPY package*.json ./
RUN npm ci --include=dev

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

CMD ["npm", "run", "start"]

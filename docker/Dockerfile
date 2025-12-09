# Stage 1: builder
FROM golang:1.22 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN GOOS=linux GOARCH=amd64 go build -trimpath -o /out/bin/generator ./cmd/generator
RUN GOOS=linux GOARCH=amd64 go build -trimpath -o /out/bin/sorter ./cmd/sorter
RUN GOOS=linux GOARCH=amd64 go build -trimpath -o /out/bin/verifier ./cmd/verifier

# Stage 2: runtime
FROM debian:12-slim AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg2 tar xz-utils netcat-openbsd procps \
  && rm -rf /var/lib/apt/lists/*
ARG REDPANDA_VER=v23.1.9
RUN set -eux; \
    curl -sSL "https://github.com/redpanda-data/redpanda/releases/download/${REDPANDA_VER}/redpanda-${REDPANDA_VER}-linux-x86_64.tar.gz" -o /tmp/rp.tar.gz; \
    tar -xzf /tmp/rp.tar.gz -C /opt; \
    ln -s /opt/redpanda-${REDPANDA_VER}/bin/redpanda /usr/bin/redpanda; \
    ln -s /opt/redpanda-${REDPANDA_VER}/bin/rpk /usr/bin/rpk; \
    rm /tmp/rp.tar.gz

WORKDIR /app
COPY --from=builder /out/bin/ ./bin/
COPY scripts/start.sh /start.sh
RUN chmod +x /start.sh /app/bin/*
VOLUME ["/data"]
EXPOSE 9092 33145
ENV GOMAXPROCS=4
ENTRYPOINT ["/start.sh"]
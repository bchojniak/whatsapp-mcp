# syntax=docker/dockerfile:1

FROM golang:1.26.5-bookworm AS bridge-build
WORKDIR /src/whatsapp-bridge
COPY whatsapp-bridge/go.mod whatsapp-bridge/go.sum ./
RUN go mod download
COPY whatsapp-bridge/*.go ./
RUN CGO_ENABLED=1 go build -tags sqlite_fts5 -trimpath -ldflags="-s -w" -o /out/whatsapp-bridge .

FROM debian:bookworm-slim AS tunnel-client-download
ARG TARGETARCH
ARG TUNNEL_CLIENT_VERSION=v0.0.10
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "$TARGETARCH" in \
        amd64) release_arch='amd64' ;; \
        arm64) release_arch='arm64' ;; \
        *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    release_base="https://github.com/openai/tunnel-client/releases/download/${TUNNEL_CLIENT_VERSION}"; \
    curl -fsSLo /tmp/SHA256SUMS.txt "${release_base}/SHA256SUMS.txt"; \
    asset="$(awk '{name=$2; sub(/^\\*/, "", name); print name}' /tmp/SHA256SUMS.txt \
        | grep -E "linux[_-]${release_arch}\\.zip$" | head -n 1)"; \
    test -n "$asset"; \
    curl -fsSLo "/tmp/$asset" "${release_base}/${asset}"; \
    expected="$(awk -v asset="$asset" '{name=$2; sub(/^\\*/, "", name); if (name == asset) print $1}' /tmp/SHA256SUMS.txt)"; \
    test -n "$expected"; \
    (cd /tmp && echo "$expected  $asset" | sha256sum -c -); \
    mkdir -p /tmp/tunnel-client; \
    unzip -q "/tmp/$asset" -d /tmp/tunnel-client; \
    find /tmp/tunnel-client -type f -name tunnel-client -exec cp '{}' /out-tunnel-client \;; \
    test -x /out-tunnel-client || chmod +x /out-tunnel-client

FROM python:3.12-slim-bookworm AS runtime
COPY --from=ghcr.io/astral-sh/uv:0.8.15 /uv /uvx /bin/
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl gosu \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 whatsapp \
    && useradd --uid 10001 --gid whatsapp --home-dir /app --shell /usr/sbin/nologin whatsapp

WORKDIR /app/whatsapp-mcp-server
COPY README.md /app/README.md
COPY whatsapp-mcp-server/pyproject.toml whatsapp-mcp-server/uv.lock ./
RUN uv sync --frozen --no-dev
COPY whatsapp-mcp-server/*.py ./

COPY --from=bridge-build /out/whatsapp-bridge /app/bin/whatsapp-bridge
COPY --from=tunnel-client-download /out-tunnel-client /usr/local/bin/tunnel-client
COPY --from=docker.io/tailscale/tailscale:v1.102.2 /usr/local/bin/tailscaled /usr/local/bin/tailscale /usr/local/bin/
COPY deploy/entrypoint.sh /app/bin/entrypoint.sh
COPY deploy/backup_store.py /app/bin/backup_store.py
RUN chmod +x /app/bin/entrypoint.sh /app/bin/whatsapp-bridge /usr/local/bin/tunnel-client /usr/local/bin/tailscale /usr/local/bin/tailscaled \
    && mkdir -p /data \
    && chown -R whatsapp:whatsapp /app /data \
    && chmod 0700 /data

ENV WHATSAPP_STORE_DIR=/data \
    WHATSAPP_MESSAGES_DB_PATH=/data/messages.db \
    WHATSAPP_API_LISTEN_ADDR=127.0.0.1:8080 \
    WHATSAPP_API_BASE_URL=http://127.0.0.1:8080/api \
    WHATSAPP_MCP_READ_ONLY=true \
    WHATSAPP_ALLOW_LOG_QR=false \
    WHATSAPP_MCP_TRANSPORT=streamable-http \
    WHATSAPP_MCP_HOST=127.0.0.1 \
    WHATSAPP_MCP_PORT=8000 \
    HEALTH_LISTEN_ADDR=127.0.0.1:8081 \
    OPEN_WEB_UI=false

ENTRYPOINT ["/app/bin/entrypoint.sh"]

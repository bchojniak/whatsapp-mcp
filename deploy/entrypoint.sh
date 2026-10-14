#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

if [[ "$(id -u)" -eq 0 ]]; then
    mkdir -p /data
    chown -R whatsapp:whatsapp /data
    find /data -xdev -type d -exec chmod 0700 {} +
    find /data -xdev -type f -exec chmod 0600 {} +
    exec gosu whatsapp "$0" "$@"
fi

bridge_pid=""
mcp_pid=""
tunnel_pid=""
tailscaled_pid=""
backup_ready_file=""

shutdown() {
    local status="${1:-0}"
    local pid
    trap - INT TERM EXIT
    for pid in "$tunnel_pid" "$mcp_pid" "$bridge_pid" "$tailscaled_pid"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$tunnel_pid" "$mcp_pid" "$bridge_pid" "$tailscaled_pid"; do
        [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true
    done
    [[ -z "$backup_ready_file" ]] || rm -f -- "$backup_ready_file"
    exit "$status"
}

trap 'shutdown 143' INT TERM

if [[ "${WHATSAPP_MIGRATION_MODE:-false}" == "true" ]]; then
    echo "Migration mode is active; the worker is idle and /data is available for file transfer"
    exec sleep infinity
fi

tunnel_enabled="$(/app/whatsapp-mcp-server/.venv/bin/python /app/whatsapp-mcp-server/access_control.py --print-tunnel-enabled)"
if [[ "$tunnel_enabled" == "true" ]]; then
    if [[ -z "${CONTROL_PLANE_API_KEY:-}" ]]; then
        echo "CONTROL_PLANE_API_KEY is required" >&2
        exit 1
    fi

    CONTROL_PLANE_TUNNEL_ID="${CONTROL_PLANE_TUNNEL_ID:-${OPENAI_TUNNEL_ID:-}}"
    if [[ -z "$CONTROL_PLANE_TUNNEL_ID" ]]; then
        echo "OPENAI_TUNNEL_ID or CONTROL_PLANE_TUNNEL_ID is required" >&2
        exit 1
    fi
    export CONTROL_PLANE_TUNNEL_ID
fi

echo "Verifying private SQLite backups before starting the WhatsApp bridge"
backup_ready_file="$(mktemp /tmp/whatsapp-backup-ready.XXXXXX)"
backup_args=(--store-dir "${WHATSAPP_STORE_DIR:-/data}" --ready-file "$backup_ready_file")
if [[ "${WHATSAPP_ALLOW_FRESH_STORE:-false}" != "true" ]]; then
    backup_args+=(--require-existing-databases)
fi
/app/whatsapp-mcp-server/.venv/bin/python /app/bin/backup_store.py "${backup_args[@]}" \
    --bridge /app/bin/whatsapp-bridge &
bridge_pid=$!

backup_timeout="${WHATSAPP_BACKUP_START_TIMEOUT:-1500}"
backup_deadline=$((SECONDS + backup_timeout))
until [[ "$(cat "$backup_ready_file")" == "verified" ]]; do
    if ! kill -0 "$bridge_pid" 2>/dev/null; then
        wait "$bridge_pid" || true
        echo "SQLite backup gate failed; the bridge was not started" >&2
        shutdown 1
    fi
    if (( SECONDS >= backup_deadline )); then
        echo "SQLite backup gate exceeded its startup budget; operator review is required" >&2
        shutdown 1
    fi
    sleep 1
done
rm -f -- "$backup_ready_file"
backup_ready_file=""

bridge_url="${WHATSAPP_API_BASE_URL%/}/health"
start_timeout="${WHATSAPP_BRIDGE_START_TIMEOUT:-300}"
deadline=$((SECONDS + start_timeout))

until curl --fail --silent --max-time 2 "$bridge_url" >/dev/null; do
    if ! kill -0 "$bridge_pid" 2>/dev/null; then
        wait "$bridge_pid" || true
        echo "WhatsApp bridge exited before its API became ready" >&2
        exit 1
    fi
    if (( SECONDS >= deadline )); then
        echo "WhatsApp bridge API did not become ready within ${start_timeout}s" >&2
        shutdown 1
    fi
    sleep 1
done

echo "WhatsApp bridge API is ready; starting local Streamable HTTP MCP server"
export WHATSAPP_MCP_TRANSPORT="${WHATSAPP_MCP_TRANSPORT:-streamable-http}"
export WHATSAPP_MCP_HOST="${WHATSAPP_MCP_HOST:-127.0.0.1}"
export WHATSAPP_MCP_PORT="${WHATSAPP_MCP_PORT:-8000}"
TS_HOSTNAME="${TS_HOSTNAME:-whatsapp-mcp}"
TS_TAILNET="${TS_TAILNET:-example.ts.net}"
if [[ -z "${WHATSAPP_MCP_ALLOWED_HOSTS:-}" ]]; then
    export WHATSAPP_MCP_ALLOWED_HOSTS="${TS_HOSTNAME}.${TS_TAILNET},${TS_HOSTNAME}.${TS_TAILNET}:*"
fi
/app/whatsapp-mcp-server/.venv/bin/python /app/whatsapp-mcp-server/main.py &
mcp_pid=$!

mcp_url="http://127.0.0.1:${WHATSAPP_MCP_PORT}/mcp"
mcp_health_url="http://127.0.0.1:${WHATSAPP_MCP_PORT}/healthz"
mcp_start_timeout="${WHATSAPP_MCP_START_TIMEOUT:-60}"
mcp_deadline=$((SECONDS + mcp_start_timeout))

until curl --fail --silent --output /dev/null --max-time 2 "$mcp_health_url"; do
    if ! kill -0 "$mcp_pid" 2>/dev/null; then
        wait "$mcp_pid" || true
        echo "WhatsApp MCP server exited before becoming ready" >&2
        shutdown 1
    fi
    if (( SECONDS >= mcp_deadline )); then
        echo "WhatsApp MCP server did not become ready within ${mcp_start_timeout}s" >&2
        shutdown 1
    fi
    sleep 1
done

if [[ -n "${TS_AUTHKEY:-}" ]]; then
    echo "Starting Tailscale userspace node for tailnet-only MCP access"
    mkdir -p /data/tailscale
    chmod 0700 /data/tailscale
    tailscaled \
        --tun=userspace-networking \
        --statedir=/data/tailscale \
        --socket=/tmp/tailscaled.sock \
        --port=41641 &
    tailscaled_pid=$!
    for _ in $(seq 1 30); do
        if [[ -S /tmp/tailscaled.sock ]]; then
            break
        fi
        sleep 0.2
    done
    tailscale --socket=/tmp/tailscaled.sock up \
        --auth-key="${TS_AUTHKEY}" \
        --hostname="${TS_HOSTNAME:-whatsapp-mcp}" \
        --accept-dns=false \
        --accept-routes=false \
        --ssh=false \
        --advertise-exit-node=false
    tailscale --socket=/tmp/tailscaled.sock serve --bg http://127.0.0.1:8000
    echo "Tailscale serve is up for hostname ${TS_HOSTNAME:-whatsapp-mcp} (tailnet only, not Funnel)"
else
    echo "TS_AUTHKEY is unset; skipping Tailscale"
fi

if [[ "$tunnel_enabled" == "true" ]]; then
    echo "WhatsApp MCP server is ready; starting OpenAI Secure MCP Tunnel"
    export MCP_SERVER_URL="${MCP_SERVER_URL:-$mcp_url}"
    unset MCP_COMMAND
    tunnel-client run \
        --mcp.extra-headers "X-WhatsApp-MCP-Key: env:WHATSAPP_MCP_TUNNEL_KEY" \
        --mcp.discovery-extra-headers "X-WhatsApp-MCP-Key: env:WHATSAPP_MCP_TUNNEL_KEY" &
    tunnel_pid=$!
else
    echo "WhatsApp MCP server is ready; OpenAI tunnel is disabled"
fi

worker_pids=("$bridge_pid" "$mcp_pid")
if [[ -n "$tunnel_pid" ]]; then
    worker_pids+=("$tunnel_pid")
fi
if [[ -n "$tailscaled_pid" ]]; then
    worker_pids+=("$tailscaled_pid")
fi
set +e
wait -n "${worker_pids[@]}"
status=$?
set -e

if [[ "$status" -eq 0 ]]; then
    status=1
fi
echo "A critical worker process exited; stopping the remaining process" >&2
shutdown "$status"

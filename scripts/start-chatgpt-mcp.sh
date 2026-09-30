#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p gmapsdata

if ! docker ps --format '{{.Names}}' | grep -qx 'gmaps-chatgpt-backend'; then
  docker rm -f gmaps-chatgpt-backend >/dev/null 2>&1 || true
  docker run -d \
    --name gmaps-chatgpt-backend \
    -p 8080:8080 \
    -v "$ROOT/gmapsdata:/gmapsdata" \
    gosom/google-maps-scraper \
    -data-folder /gmapsdata
fi

if [ ! -d .mcp-venv ]; then
  python3 -m venv .mcp-venv
fi

.mcp-venv/bin/python -m pip install -q -r mcp/requirements.txt

echo "Google Maps backend: http://127.0.0.1:8080"
echo "MCP endpoint:        http://127.0.0.1:8765/mcp"
echo "Keep this terminal open."

exec npx -y supergateway \
  --stdio "$ROOT/.mcp-venv/bin/python $ROOT/mcp/server.py" \
  --outputTransport streamableHttp \
  --port 8765 \
  --streamableHttpPath /mcp

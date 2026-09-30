# ChatGPT local MCP

This branch adds a thin MCP adapter without changing the scraper engine.

## What runs

1. The existing Google Maps Scraper web/API runs locally on port 8080.
2. `mcp/server.py` exposes short MCP job tools over stdio.
3. Supergateway converts stdio MCP to Streamable HTTP at `http://127.0.0.1:8765/mcp`.

Tools:
- `health`
- `start_scrape`
- `list_jobs`
- `get_job`
- `get_results`

## Mac quick start

Requirements: Docker/OrbStack, Python 3, Node/npm.

```bash
git checkout chatgpt-mcp-local
chmod +x scripts/start-chatgpt-mcp.sh
./scripts/start-chatgpt-mcp.sh
```

The MCP stays local. To use it from ChatGPT web, connect the local HTTP MCP
through OpenAI Secure MCP Tunnel (preferred) or another HTTPS tunnel.

No Railway, Supabase, or external database is required by this adapter.

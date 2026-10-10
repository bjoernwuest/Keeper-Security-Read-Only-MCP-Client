# Keeper Security Read-Only MCP Client

Read-only [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) bridge to [Keeper Security Vault](https://www.keepersecurity.com/) via `keepercommander`. Exposes passwords, API keys, 2FA/TOTP codes and secrets as MCP tools — strictly scoped to allowed Keeper folders.

Source: `keeper-mcp-server.py` (`/docker/litellm/keeper-mcp-server.py:1`), Docker: `Dockerfile`.

## Features

- **Folder-scoped:** one MCP instance per `ALLOWED_FOLDERS` entry at `<host>/<folder>/mcp`. Unknown folders → 404. Empty `ALLOWED_FOLDERS` → unrestricted `/mcp` (not recommended).
- **4 read-only tools:** `list_secrets`, `search_secrets`, `get_secret(record_uid)`, `get_totp(record_uid)`. No write access.
- **UID-only access:** `get_secret`/`get_totp` require the 22-char `record_uid` (`/docker/litellm/keeper-mcp-server.py:189`), titles are rejected. Access outside the allowed folder → `PermissionError`.
- **In-memory cache with fallback:** answers from cache if Keeper exceeds `KEEPER_TIMEOUT_SECONDS` (`/docker/litellm/keeper-mcp-server.py:98`). Periodic full refresh + `GET /cache`, `GET /health`.
- **Session handling:** auto re-login on expired session (`KEEPER_USERNAME` + `KEEPER_PASSWORD`), periodic `sync-down` keepalive.

## Quickstart

```bash
docker build -t keeper-mcp .
docker run --rm -p 8000:8000 \
  -e ALLOWED_FOLDERS="AI - OpenCode - Server" \
  -e KEEPER_USERNAME="user@example.com" \
  -e KEEPER_PASSWORD="..." \
  keeper-mcp
```

Open `http://localhost:8000/` for endpoints, `http://localhost:8000/health` for health.

### Docker Compose

```yaml
services:
  keeper-mcp:
    build: .
    ports: ["8000:8000"]
    environment:
      ALLOWED_FOLDERS: "AI - OpenCode - Server"
      KEEPER_USERNAME: "${KEEPER_USERNAME}"
      KEEPER_PASSWORD: "${KEEPER_PASSWORD}"
      CACHE_REFRESH_SECONDS: "3600"
      KEEPER_TIMEOUT_SECONDS: "5"
      KEEPALIVE_HOURS: "4"
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `ALLOWED_FOLDERS` | `""` | Comma-separated Keeper folders. Empty = unrestricted. |
| `KEEPER_USERNAME` | — | Login user for re-login (fallback: `config.json`). |
| `KEEPER_PASSWORD` | — | Login password for re-login. |
| `KEEPER_CONFIG_FILE` | — | Path to Keeper `config.json`. |
| `CACHE_REFRESH_SECONDS` / `CACHE_REFRESH_HOURS` | `3600` / `1.0` | Full cache refresh interval. |
| `KEEPER_TIMEOUT_SECONDS` | `5` | Per-request Keeper timeout before cache fallback. |
| `KEEPALIVE_HOURS` | `4` | `sync-down` interval. |
| `HOST` / `PORT` | `0.0.0.0` / `8000` | HTTP bind. |

## MCP usage (OpenCode example)

```json
{
  "mcp": {
    "keeper": {
      "type": "streamable",
      "url": "http://localhost:8000/AI%20-%20OpenCode%20-%20Server/mcp"
    }
  }
}
```

Then:

1. `list_secrets()` → `{folder, records: [{record_uid, title, type, details}]}`.
2. `search_secrets("GitHub")` → filtered to the allowed folder.
3. `get_secret("<22-char record_uid>")` → full JSON details.
4. `get_totp("<22-char record_uid>")` → `{record_uid, totp}`. Retry up to 3x if the 30s code expired.

## Endpoints

- `GET /` — service + mounted `<folder>/mcp` endpoints.
- `GET /health` — `{status, cache entries, last_refresh}`.
- `GET /cache` — cache keys + refresh state.
- `<folder>/mcp` — per-folder MCP (Streamable HTTP).

## Security

- Read-only: only `ls`, `search`, `get`, `totp`, `whoami`, `sync-down` are executed; passwords are redacted in logs (`/docker/litellm/keeper-mcp-server.py:245`).
- Publish scope: this repo contains only the MCP server + Dockerfile. Never commit `config.json`, `KEEPER_PASSWORD` or Vault dumps.

## License

MIT — see [LICENSE.md](LICENSE.md).

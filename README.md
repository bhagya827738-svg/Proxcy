# Standalone CCC Proxy

Lightweight Cloudflare Worker proxy that injects Claude Code CLI (CCC) headers and cleans payload parameters. Self-contained: `proxy.js` has no imports, so this folder can be zipped and deployed on its own.

## Features
- **Setup page at `/`** — open the worker in a browser and it prints its own
  base URL plus copy-paste config for Claude Code, OpenAI-compatible clients,
  and curl. Works unchanged locally (`http://localhost:8787`) and deployed.
- Injects `User-Agent: claude-cli/1.0.0 (external, cli)` and `anthropic-version`.
- Strips invalid request parameters (`temperature`, `top_p`).
- Fixes `properties: []` schema issues and orphan tool call messages.
- Supports both streaming SSE and aggregated JSON completions.
- **Anthropic SSE handling** on `/v1/messages`: waits for the upstream
  `message_start` event before committing the response, retries once if the
  stream dies before it arrives, and strips standalone `data: null` events that
  break strict Anthropic clients.
- Retries up to 8 times with capped backoff, including upstream errors that
  arrive with a `200` status inside the stream body.
- Sanitizes lone surrogates so malformed UTF-16 never reaches the client.

## How to Run

### 1. Local Development
```bash
npm install
npm run dev
```
Then open <http://localhost:8787> for the setup page.

### 2. Deploy to Cloudflare Workers
```bash
npx wrangler login
npm run deploy
```
The setup page is then at your worker's root URL.

## Environment Variables (Optional)
In `wrangler.toml` or Cloudflare dashboard:
- `UPSTREAM_ORIGIN`: Target host (default: `https://agentrouter.org`).
- `API_KEY`: Fallback API key if clients do not supply their own `Authorization` or `x-api-key` header.

# Bridger — WebSocket Relay System for Remote Ollama

A dual-purpose relay system with two use cases:

1. **Simple API Proxy** — `POST /bridge` forwards HTTP requests to any endpoint (original feature)
2. **WebSocket Relay for Remote Ollama** — Lets you use a **remote Ollama instance** from VSCode (via Cline) or any OpenAI-compatible client across different networks

## Architecture

```
LLM-Client Machine (VSCode + Cline)
  │
  │  HTTP (OpenAI-compatible API)
  ▼
client-proxy/ (Express + WebSocket Client)
  │
  │  WebSocket (WSS)
  ▼
Relay Server (Heroku — bridger)
  │  • Routes REQUEST/TOKEN/COMPLETE/ERROR/CANCEL
  │  • Admin panel at /ws-admin for monitoring
  │  WebSocket (WSS)
  ▼
server-agent/ (WebSocket Client + Ollama)
  │  • Supports cancellation (aborts Ollama HTTP request)
  │  HTTP (Ollama API)
  ▼
Ollama (local LLM)
```

## Components

### 1. Relay Server (`src/server.js` + `src/websocketServer.js`)
Central relay that routes messages between clients and servers.

| Endpoint | Purpose |
|----------|---------|
| `POST /bridge` | Generic HTTP proxy (original feature) |
| `GET /health` | Health check |
| `/ws` | WebSocket endpoint for relay protocol |
| `/ws-admin` | Admin panel (password-protected) |

**Admin panel** at `/ws-admin` — real-time dashboard showing connected clients, servers, pending requests, event log, and ability to disconnect individual/all connections.

### 2. Server-Agent (`server-agent/`)
Runs on the machine with Ollama. Connects to the relay, receives prompts, calls Ollama, and streams back responses.

```bash
RELAY_URL=wss://your-app.herokuapp.com node server-agent/index.js
```

| Variable | Default | Description |
|----------|---------|-------------|
| `RELAY_URL` | `ws://localhost:3000` | WebSocket URL of the relay |
| `SERVER_ID` | Auto-generated | Optional server identifier |
| `RELAY_SECRET` | — | Shared secret for authentication |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama server URL |

### 3. Client-Proxy (`client-proxy/`)
Runs on the dev machine. Exposes an OpenAI-compatible HTTP API and connects to the relay via WebSocket.

```bash
RELAY_URL=wss://your-app.herokuapp.com node client-proxy/index.js
```

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | Local HTTP port |
| `RELAY_URL` | `ws://localhost:3000` | WebSocket URL of the relay |
| `CLIENT_ID` | Auto-generated | Client identifier |
| `API_KEY` | `dummy` | API key that Cline must send |

**Endpoints:** `POST /v1/chat/completions` (streaming + non-streaming), `GET /v1/models`, `GET /health`

### 4. Shared WebSocket Client (`src/relayClient.js`)
Reusable library handling connection, registration, reconnection, and the relay protocol.

## Quick Start

```bash
# 1. Start the relay server
npm install
npm start                    # http://localhost:3000

# 2. On the Ollama machine, start the server-agent
npm run server-agent

# 3. On the dev machine, start the client-proxy
npm run client-proxy

# 4. Configure Cline
#    API Provider: OpenAI Compatible
#    Base URL: http://localhost:3000/v1
#    API Key: dummy
```

## npm Scripts

```bash
npm start                 # Start the relay server
npm run dev               # Start relay with auto-reload
npm run server-agent      # Start the server-agent (Ollama machine)
npm run client-proxy      # Start the client-proxy (dev machine)
npm test                  # Run relay unit tests (12 tests)
npm run test:integration  # Run integration test with mock Ollama (7 tests)
npm run test:ollama       # Run integration test against real Ollama
```

## Admin Panel

Open `http://localhost:3000/ws-admin` in a browser. Enter the admin password (default: `abc123`, configurable via `ADMIN_PASSWORD` env var).

- Real-time status of clients, servers, and pending requests
- Disconnect individual clients/servers or all at once
- Scrollable event log

## Streaming Support

| Mode | What happens |
|------|-------------|
| `stream: true` | Tokens arrive individually via WebSocket → SSE events |
| `stream: false` | Full response sent as one WebSocket message → single JSON |

## Deploy to Heroku

```bash
heroku create your-bridger-app
heroku config:set RELAY_SECRET=your-shared-secret
heroku config:set ADMIN_PASSWORD=your-admin-password
git push heroku main
```

## Project Structure

```
bridger/
├── src/
│   ├── server.js              # Express + HTTP server (relay)
│   ├── websocketServer.js      # WebSocket relay protocol + admin role
│   ├── relayClient.js          # Shared WS client library
│   ├── server-agent/
│   │   ├── index.js            # Server-agent entry point (with CANCEL support)
│   │   └── ollamaClient.js     # Ollama API client (streaming + abort support)
│   └── client-proxy/
│       ├── index.js            # Client-proxy entry point
│       └── openaiAdapter.js    # OpenAI format converter (SSE + JSON)
├── public/
│   ├── index.html              # Browser playground for /bridge proxy
│   └── ws-admin.html           # Admin panel (monitoring & control)
├── tests/
│   ├── relay.unit.js           # 12 relay server unit tests (incl. admin)
│   └── integration.mock.js     # 7 full pipeline integration tests
├── test-real-ollama.js          # Real Ollama integration test
├── package.json
├── Procfile                    # Heroku deployment
└── README.md
```

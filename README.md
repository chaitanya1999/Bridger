# Bridger — WebSocket Relay System for Remote Ollama

A dual-purpose relay system with two use cases:

1. **Simple API Proxy** — `POST /bridge` forwards HTTP requests to any endpoint (original feature)
2. **WebSocket Relay for Remote Ollama** — Lets you use a **remote Ollama instance** from VSCode (via Cline) or any OpenAI-compatible client across different networks

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  CLIENT MACHINE (Your dev machine with VSCode)                  │
│                                                                 │
│  VSCode + Cline Plugin                                          │
│       │                                                         │
│       │  HTTP request (OpenAI-compatible format)                │
│       │  e.g. POST http://localhost:3001/v1/chat/completions    │
│       ▼                                                         │
│  src/client-proxy/index.js (Express + WebSocket Client)         │
│    - Exposes OpenAI-compatible HTTP API                         │
│    - Translates HTTP ↔ WebSocket relay protocol                 │
│    - Connects to relay server via WebSocket                     │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       │  WebSocket (WSS) — persistent connection
                       │  "I am LLM_CLIENT, send me responses"
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  HEROKU / CLOUD SERVER (Bridger Relay)                          │
│                                                                 │
│  src/server.js + src/websocketServer.js                         │
│    - Routes messages between clients and servers                │
│    - No Ollama, no LLM — just a smart message relay             │
│    - Handles registration, routing, cleanup, heartbeats         │
│    - Multiple clients/servers can connect simultaneously        │
│    - Admin panel at /ws-admin for monitoring & control          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       │  WebSocket (WSS) — persistent connection
                       │  "I am LLM_SERVER, send me prompts"
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│  SERVER MACHINE (Your laptop/desktop with Ollama)               │
│                                                                 │
│  src/server-agent/index.js (WebSocket Client + Ollama Client)   │
│    - Connects to relay server                                   │
│    - Receives LLM prompts via WebSocket                         │
│    - Supports cancellation (aborts Ollama HTTP request)          │
│       │                                                         │
│       │  HTTP request (Ollama API)                              │
│       ▼                                                         │
│  Ollama (http://localhost:11434)                                 │
│    - Runs LLM models (llama3, gemma4, mistral, etc.)           │
│    - Returns tokens (streaming or full response)                │
└─────────────────────────────────────────────────────────────────┘
```

## How It Works

1. You write a prompt in Cline (VSCode)
2. Cline sends an OpenAI-format request to **client-proxy** (`http://localhost:3001/v1/chat/completions`)
3. **client-proxy** converts it to a WebSocket `REQUEST` message and sends it to the **relay server** (Heroku)
4. The **relay server** forwards the `REQUEST` to the **server-agent** (your Ollama machine)
5. **server-agent** calls Ollama's API with the prompt
6. Ollama generates tokens — **server-agent** sends each token back as a WebSocket `TOKEN` message via the relay
7. The relay forwards `TOKEN` messages to **client-proxy**
8. **client-proxy** converts tokens to OpenAI SSE stream (or single JSON response) and sends back to Cline
9. Cline displays the response in VSCode

## Admin Panel (`/ws-admin`)

The relay server includes an admin panel for monitoring and controlling connections.

**Access:** Open `http://localhost:3000/ws-admin` in a browser and enter the admin password.

**Features:**
- Real-time status dashboard (updated every 2 seconds):
  - Connected LLM-Clients (with disconnect buttons)
  - Connected LLM-Servers (with disconnect buttons)
  - Pending requests (with age tracking)
- Event log — scrollable real-time log of all relay activity
- **Disconnect All** button — disconnects all clients/servers and cancels pending requests
- **Clear Log** button
- **Reconnect** button
- Auto-reconnects on connection loss (3s interval)

**Environment variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `ADMIN_PASSWORD` | `abc123` | Password required by admin panel |

## Setup Guide (3 Machines)

### 🖥️ Machine 1: Heroku / Cloud Server (The Relay)

This is the central relay server. It must be publicly accessible so both the client and server machines can connect to it.

**Deploy to Heroku:**
```bash
# Clone the repo
git clone <your-repo-url>
cd bridger

# Create a Heroku app
heroku create your-bridger-app

# Set an optional shared secret (recommended for security)
heroku config:set RELAY_SECRET=your-shared-secret

# Set admin panel password (recommended)
heroku config:set ADMIN_PASSWORD=your-admin-password

# Deploy
git push heroku main
```

**Or run locally for testing:**
```bash
npm install
npm start
# Relay runs on http://localhost:3000
```

**Verify it's running:**
```bash
curl https://your-bridger-app.herokuapp.com/health
# → {"ok":true}
```

---

### 🖥️ Machine 2: Server Machine (The one with Ollama)

This is the machine that has Ollama installed and models downloaded. It connects to the relay server and waits for prompts.

**Prerequisites:**
- Node.js 20+
- Ollama installed and running (`http://localhost:11434`)
- At least one model pulled (e.g., `ollama pull llama3`)

**Setup:**
```bash
# On the server machine, clone or copy the bridger project
cd bridger
npm install

# Start the server-agent
# If relay is on Heroku:
RELAY_URL=wss://your-bridger-app.herokuapp.com \
RELAY_SECRET=your-shared-secret \
node src/server-agent/index.js

# If relay is running locally for testing:
# RELAY_URL=ws://localhost:3000 node src/server-agent/index.js
```

**Expected output:**
```
[Server-Agent] Starting — relay: wss://your-bridger-app.herokuapp.com, ollama: http://localhost:11434
[Server-Agent] Connected to relay
[Server-Agent] Registered as LLM-Server: server-1717765432100
```

**Environment variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `RELAY_URL` | `ws://localhost:3000` | WebSocket URL of the relay server |
| `SERVER_ID` | Auto-generated | Unique ID for this server (e.g., `my-laptop-ollama`) |
| `RELAY_SECRET` | — | Must match the relay's `RELAY_SECRET` for authentication |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama server URL |

**Keep this terminal running** — it needs to maintain the WebSocket connection.

---

### 🖥️ Machine 3: Client Machine (Your dev machine with VSCode + Cline)

This is where you write code. It runs a small local Express server that Cline connects to.

**Prerequisites:**
- Node.js 20+
- VSCode with Cline extension installed

**Setup:**
```bash
# On your dev machine, clone or copy the bridger project
cd bridger
npm install

# Start the client-proxy
# If relay is on Heroku:
RELAY_URL=wss://your-bridger-app.herokuapp.com \
PORT=3001 \
node src/client-proxy/index.js

# If relay is running locally for testing:
# PORT=3001 node src/client-proxy/index.js
```

**Expected output:**
```
[Client-Proxy] Connected to relay
[Client-Proxy] Registered as LLM-Client: client-1717765432123
[Client-Proxy] HTTP server listening on port 3001
[Client-Proxy] OpenAI-compatible endpoint: http://localhost:3001/v1
[Client-Proxy] Expected API key: dummy
```

**Environment variables:**
| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | Local HTTP port (use `3001` if relay is also running locally) |
| `RELAY_URL` | `ws://localhost:3000` | WebSocket URL of the relay server |
| `CLIENT_ID` | Auto-generated | Unique ID for this client |
| `API_KEY` | `dummy` | API key that Cline must send |

**Configure Cline:**

1. Open VSCode → Cline extension settings
2. Set **API Provider** to `OpenAI Compatible`
3. Set **Base URL** to: `http://localhost:3001/v1`
4. Set **API Key** to: `dummy`
5. Set **Model** to any model available on your Ollama (e.g., `llama3`, `gemma4`, `mistral`)

That's it! Now when you write a prompt in Cline:
1. Cline sends it to `http://localhost:3001/v1/chat/completions`
2. The client-proxy forwards it via WebSocket through the relay
3. The server-agent receives it and calls Ollama
4. The response streams back through the entire chain
5. Cline displays the response in VSCode

---

## Streaming vs Non-Streaming

The system works with both modes automatically:

| Mode | What happens | Cline sees |
|------|-------------|------------|
| `stream: true` | Each token from Ollama is sent individually via WebSocket. Client-proxy converts to SSE. | Tokens appear one by one (faster perceived response) |
| `stream: false` | Ollama generates the full response, then sends it as a single WebSocket message. Client-proxy returns a single JSON. | Full response appears at once |

Cline controls this — the relay system adapts automatically.

## Quick Reference: npm Scripts

```bash
npm start                 # Start the relay server (Heroku/cloud)
npm run dev               # Start relay with auto-reload (--watch)
npm run server-agent      # Start the server-agent (Ollama machine)
npm run client-proxy      # Start the client-proxy (dev machine)
npm test                  # Run relay server unit tests (12 tests)
npm run test:integration  # Run full integration test with mock Ollama (7 tests)
npm run test:ollama       # Run integration test against real Ollama (skips if not running)
```

## Testing Without Cline

You can test the setup using curl:

```bash
# Test non-streaming
curl -X POST http://localhost:3001/v1/chat/completions \
  -H "Authorization: Bearer dummy" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3",
    "messages": [{"role": "user", "content": "Hello! What is 2+2?"}],
    "stream": false
  }'

# Test streaming (watch tokens arrive)
curl -X POST http://localhost:3001/v1/chat/completions \
  -H "Authorization: Bearer dummy" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3",
    "messages": [{"role": "user", "content": "Count from 1 to 5"}],
    "stream": true
  }'
```

## Original Feature: HTTP Proxy (`POST /bridge`)

The original HTTP proxy still works as before:

```bash
curl -X POST https://your-bridger-app.herokuapp.com/bridge \
  -H "Content-Type: application/json" \
  -d '{
    "endpoint": "https://api.example.com/data",
    "method": "GET"
  }'
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
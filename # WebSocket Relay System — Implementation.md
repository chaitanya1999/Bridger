# WebSocket Relay System — Implementation Plan V2

## Goal

Build a distributed relay system allowing Cline (VSCode) to use a remote Ollama instance through an OpenAI-compatible API.

Target flow:

Cline
→ Express API
→ WebSocket Relay
→ Remote Agent
→ Ollama
→ Stream back

Existing Heroku Express project MUST be reused.

---

# Phase 0 — Repository Structure

project-root/

server/

* index.js
* express/
* websocket/
* relay/
* auth/
* routes/
* services/

remote-agent/

* index.js
* ollama-client.js
* websocket-client.js

shared/

* protocol.js
* constants.js

---

# Phase 1 — Upgrade Existing Express App

Requirements:

Reuse existing HTTP server.

Create:

/v1/chat/completions
/v1/models

Add WebSocket endpoint:

/relay

Implementation:

const app = express()

const server = http.createServer(app)

attach websocket server

listen(port)

---

# Phase 2 — Build Relay Layer

Maintain:

connections = {
clients: Map(),
servers: Map(),
requests: Map()
}

Request registry:

requestId:
{
clientId,
serverId,
createdAt
}

Connection registration:

{
type:"REGISTER",
role:"CLIENT"
}

{
type:"REGISTER",
role:"SERVER",
secret:"..."
}

Relay responsibilities:

* authenticate
* assign server
* route messages
* cleanup dead requests

---

# Phase 3 — Define Protocol

Message types:

REGISTER
REQUEST
TOKEN
COMPLETE
ERROR
CANCEL
PING
PONG

REQUEST

{
requestId,
model,
messages,
stream
}

TOKEN

{
requestId,
delta
}

COMPLETE

{
requestId
}

ERROR

{
requestId,
message
}

---

# Phase 4 — Remote Agent

Startup:

connect relay

register server

listen for REQUEST

Execution:

receive request
→ call Ollama
→ stream chunks
→ emit TOKEN

Completion:

emit COMPLETE

Support:

timeout
reconnect
cancel

Ollama endpoint:

POST /api/chat

stream=true

---

# Phase 5 — Express OpenAI Adapter

Receive:

POST /v1/chat/completions

Convert:

OpenAI
→ internal REQUEST

Send to relay.

Receive TOKEN.

Transform:

WS stream
→ OpenAI SSE

Output:

data:{...}

data:[DONE]

Support:

stream=true

---

# Phase 6 — Reliability

Implement:

heartbeat every 30s

request timeout 300s

max queue size 5

cleanup stale requests

auto reconnect

backpressure protection

---

# Phase 7 — Testing

Cases:

single request

multiple concurrent

cancel request

network disconnect

remote crash

large context

stream interruption

Cline integration

---

# Success Criteria

Cline points to:

http://HEROKU_URL/v1

Remote laptop runs:

node remote-agent

User writes prompt.

Tokens stream continuously from remote Ollama.

No polling.

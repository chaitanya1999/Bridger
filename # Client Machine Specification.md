# Client Machine Specification

Purpose:
Expose OpenAI-compatible API locally while forwarding execution to remote Ollama.

Processes:

1. Express API Server
2. WebSocket Client Application

---

## Process 1 — Express API Server

Folder:

client-proxy/

Responsibilities:

* expose OpenAI-compatible endpoints
* receive Cline requests
* transform requests
* call local WS client
* convert streamed responses into SSE

Endpoints:

POST /v1/chat/completions

GET /v1/models

Internal Flow:

receive request
→ generate requestId
→ send to WS client
→ wait for stream
→ stream back to Cline

Modules:

routes/
openai-adapter.js
stream-handler.js

Startup:

node client-proxy/index.js

Port:

3000

Example:

http://localhost:3000/v1

---

## Process 2 — WebSocket Client Application

Folder:

client-ws/

Responsibilities:

* maintain persistent connection to relay
* send requests
* receive streamed tokens
* map requestId
* forward events to Express

Connection:

wss://relay-server/relay

Registration:

{
role:"CLIENT",
clientId:"client-001"
}

Modules:

relay-client.js
request-manager.js
connection-manager.js

Internal APIs:

sendRequest(payload)

cancelRequest(requestId)

onToken(requestId)

onComplete(requestId)

Startup:

node client-ws/index.js

---

## Communication Between Express and WS Client

Recommended:

same process

Express:
services/wsService.js

WebSocket:
services/relaySocket.js

Architecture:

Express
↓
relaySocket.send()
↓
WebSocket

No localhost HTTP.

No Redis.

No IPC.

Single Node process.

---

## Startup

Client Machine:

node index.js

Creates:

Express Server
+
WebSocket Connection

Ready.

Configure Cline:

Base URL:

http://localhost:3000/v1

API Key:

dummy

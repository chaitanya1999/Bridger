/**
 * LLM-Client Proxy
 *
 * Provides an OpenAI-compatible HTTP API (for Cline / VSCode) and
 * connects to the bridger relay server via WebSocket to forward
 * requests to a remote LLM-Server agent.
 *
 * Usage:
 *   node client-proxy/index.js
 *
 * Environment variables:
 *   PORT            Local HTTP port (default: 3000)
 *   RELAY_URL       WebSocket URL of the relay server (default: ws://localhost:3000)
 *   CLIENT_ID       Client ID for relay registration (default: auto-generated)
 *   RELAY_SECRET    Optional shared secret for relay authentication
 *   API_KEY         Expected API key from clients (default: 'dummy')
 */

const http = require('http');
const express = require('express');
const RelayClient = require('../relayClient');
const { toRelayRequest, createSSEResponse, createJSONResponse } = require('./openaiAdapter');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:3000';
const CLIENT_ID = process.env.CLIENT_ID || `client-${Date.now()}`;
const API_KEY = process.env.API_KEY || 'dummy';

// ---- Express Setup ----

const app = express();
app.use(express.json({ limit: '10mb' }));

// Simple API key check
function authMiddleware(req, res, next) {
	const provided = req.headers['authorization']?.replace(/^Bearer\s+/i, '') || '';
	if (provided !== API_KEY) {
		return res.status(401).json({
			error: {
				message: 'Invalid API key',
				type: 'auth_error',
				param: null,
				code: 'invalid_api_key',
			},
		});
	}
	next();
}

// ---- Relay Client Setup ----

const relay = new RelayClient(RELAY_URL, {
	role: 'LLM_CLIENT',
	clientId: CLIENT_ID,
	autoReconnect: true,
});

// Map of requestId → { responseHandler, timer }
const activeRequests = new Map();

relay.on('connected', () => {
	console.log('[Client-Proxy] Connected to relay');
});

relay.on('registered', (msg) => {
	console.log(`[Client-Proxy] Registered as LLM-Client: ${msg.clientId}`);
});

relay.on('disconnected', ({ code, reason }) => {
	console.log(`[Client-Proxy] Disconnected (code=${code}, reason=${reason})`);
});

relay.on('reconnecting', ({ attempt, delay }) => {
	console.log(`[Client-Proxy] Reconnecting in ${delay}ms (attempt ${attempt})`);
});

relay.on('token', (msg) => {
	const handler = activeRequests.get(msg.requestId);
	if (handler) {
		handler.onToken(msg.requestId, msg.delta);
	}
});

relay.on('complete', (msg) => {
	const handler = activeRequests.get(msg.requestId);
	if (handler) {
		handler.onComplete(msg.requestId);
		cleanupRequest(msg.requestId);
	}
});

relay.on('error', (msg) => {
	const handler = activeRequests.get(msg.requestId);
	if (handler) {
		handler.onError(msg.requestId, msg.message);
		cleanupRequest(msg.requestId);
	}
});

relay.on('cancel', (msg) => {
	// If the server cancels (e.g., due to disconnect), notify the handler
	const handler = activeRequests.get(msg.requestId);
	if (handler) {
		handler.onError(msg.requestId, 'Request cancelled');
		cleanupRequest(msg.requestId);
	}
});

function cleanupRequest(requestId) {
	const entry = activeRequests.get(requestId);
	if (entry) {
		if (entry.timer) clearTimeout(entry.timer);
		activeRequests.delete(requestId);
	}
}

// ---- Routes ----

/**
 * POST /v1/chat/completions
 *
 * OpenAI-compatible chat completion endpoint.
 * Supports both streaming (SSE) and non-streaming responses.
 */
app.post('/v1/chat/completions', authMiddleware, async (req, res) => {
	try {
		const relayReq = toRelayRequest(req.body);
		const { model, messages, stream } = relayReq;

		// Generate a unique request ID
		const requestId = `req-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

		// Create the appropriate response handler based on streaming mode
		let handler;
		if (stream) {
			handler = createSSEResponse(res, model);
			res.writeHead(200, handler.headers);
		} else {
			handler = createJSONResponse(res, model);
			// Headers will be set by res.json() later
		}

		// Register the handler so relay events can find it
		activeRequests.set(requestId, handler);

		// Send the REQUEST through the relay
		relay.sendRequest(requestId, model, messages, stream);

		console.log(`[Client-Proxy] Sent REQUEST ${requestId} (model=${model}, stream=${stream})`);

		// Request timeout (5 minutes)
		const timer = setTimeout(() => {
			const h = activeRequests.get(requestId);
			if (h) {
				h.onError(requestId, 'Request timed out');
				cleanupRequest(requestId);
			}
		}, 300_000);
		activeRequests.get(requestId).timer = timer;

		// If the client disconnects mid-stream, send CANCEL
		req.on('close', () => {
			if (activeRequests.has(requestId)) {
				console.log(`[Client-Proxy] Client disconnected, cancelling ${requestId}`);
				relay.sendCancel(requestId);
				cleanupRequest(requestId);
			}
		});

	} catch (err) {
		console.error('[Client-Proxy] Error handling /v1/chat/completions:', err.message);
		res.status(400).json({
			error: {
				message: err.message,
				type: 'invalid_request_error',
				param: null,
				code: null,
			},
		});
	}
});

/**
 * GET /v1/models
 *
 * Returns the list of models available on the remote Ollama instance.
 * We request this via the relay by simulating a special request,
 * or we return a cached/default list.
 *
 * For now, returns a default list. The server-agent can update
 * this dynamically in the future.
 */
app.get('/v1/models', authMiddleware, (_req, res) => {
	// Return a reasonable default. In production, you could
	// cache this from a periodic /api/tags query via the server-agent.
	res.json({
		object: 'list',
		data: [
			{
				id: 'llama3',
				object: 'model',
				created: Math.floor(Date.now() / 1000),
				owned_by: 'ollama',
			},
			{
				id: 'gemma4',
				object: 'model',
				created: Math.floor(Date.now() / 1000),
				owned_by: 'ollama',
			},
			{
				id: 'mistral',
				object: 'model',
				created: Math.floor(Date.now() / 1000),
				owned_by: 'ollama',
			},
		],
	});
});

/**
 * GET /health
 */
app.get('/health', (_req, res) => {
	res.json({
		ok: true,
		relayConnected: relay.registered,
		clientId: CLIENT_ID,
	});
});

// ---- Start ----

async function main() {
	// Connect to the relay first
	try {
		await relay.connect();
	} catch (err) {
		console.error('[Client-Proxy] Failed to connect to relay:', err.message);
		console.log('[Client-Proxy] Starting HTTP server anyway (relay will auto-reconnect)...');
	}

	// Start HTTP server
	const server = http.createServer(app);
	server.listen(PORT, () => {
		console.log(`[Client-Proxy] HTTP server listening on port ${PORT}`);
		console.log(`[Client-Proxy] OpenAI-compatible endpoint: http://localhost:${PORT}/v1`);
		console.log(`[Client-Proxy] Expected API key: ${API_KEY}`);
	});

	// Graceful shutdown
	process.on('SIGINT', () => {
		console.log('\n[Client-Proxy] Shutting down...');
		relay.disconnect();
		server.close(() => process.exit(0));
	});

	process.on('SIGTERM', () => {
		relay.disconnect();
		server.close(() => process.exit(0));
	});
}

main();
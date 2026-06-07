/**
 * LLM-Server Agent
 *
 * Connects to the bridger relay server as an LLM_SERVER.
 * Receives REQUEST messages, calls Ollama, and streams back
 * TOKEN / COMPLETE / ERROR messages.
 *
 * Usage:
 *   node server-agent/index.js
 *
 * Environment variables:
 *   RELAY_URL       WebSocket URL of the relay server (default: ws://localhost:3000)
 *   SERVER_ID       Optional server ID (auto-generated if omitted)
 *   RELAY_SECRET    Optional shared secret for authentication
 *   OLLAMA_URL      Ollama server URL (default: http://localhost:11434)
 */

const RelayClient = require('../relayClient');
const OllamaClient = require('./ollamaClient');

const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:3000';
const SERVER_ID = process.env.SERVER_ID;
const RELAY_SECRET = process.env.RELAY_SECRET;
const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434';

const ollama = new OllamaClient(OLLAMA_URL);

const relay = new RelayClient(RELAY_URL, {
	role: 'LLM_SERVER',
	serverId: SERVER_ID,
	secret: RELAY_SECRET,
	autoReconnect: true,
});

// Track active requests so we can abort them on CANCEL
const activeRequests = new Map(); // requestId → AbortController

// ---- Event handlers ----

relay.on('connected', () => {
	console.log('[Server-Agent] Connected to relay');
});

relay.on('registered', (msg) => {
	console.log(`[Server-Agent] Registered as LLM-Server: ${msg.serverId}`);
});

relay.on('disconnected', ({ code, reason }) => {
	console.log(`[Server-Agent] Disconnected (code=${code}, reason=${reason})`);
});

relay.on('reconnecting', ({ attempt, delay }) => {
	console.log(`[Server-Agent] Reconnecting in ${delay}ms (attempt ${attempt})`);
});

/**
 * Handle an incoming REQUEST from a client via the relay.
 * Calls Ollama and streams back tokens.
 */
relay.on('request', async (msg) => {
	const { requestId, model, messages, stream = true } = msg;

	console.log(`[Server-Agent] Received REQUEST ${requestId} (model=${model}, stream=${stream})`);

	// Create an AbortController for this request
	const controller = new AbortController();
	activeRequests.set(requestId, controller);

	try {
		// Call Ollama — the ollamaClient handles both streaming and non-streaming modes
		// Pass the abort signal so CANCEL can stop the HTTP request
		for await (const chunk of ollama.chat(model, messages, stream, controller.signal)) {
			if (controller.signal.aborted) {
				console.log(`[Server-Agent] REQUEST ${requestId} aborted mid-stream`);
				break;
			}
			if (chunk.content) {
				relay.sendToken(requestId, { content: chunk.content });
			}
			if (chunk.done) {
				relay.sendComplete(requestId);
				console.log(`[Server-Agent] Completed REQUEST ${requestId}`);
			}
		}
	} catch (err) {
		// Don't log "Aborted" as an error — it's expected on cancel
		if (err.message === 'Aborted' || controller.signal.aborted) {
			console.log(`[Server-Agent] REQUEST ${requestId} cancelled`);
		} else {
			console.error(`[Server-Agent] Error processing REQUEST ${requestId}:`, err.message);
			relay.sendError(requestId, err.message);
		}
	} finally {
		activeRequests.delete(requestId);
	}
});

/**
 * Handle CANCEL from the relay — abort the active Ollama request.
 */
relay.on('cancel', (msg) => {
	const { requestId } = msg;
	console.log(`[Server-Agent] Received CANCEL for ${requestId}`);

	const controller = activeRequests.get(requestId);
	if (controller) {
		controller.abort();
		console.log(`[Server-Agent] Aborted Ollama request for ${requestId}`);
	}
});

relay.on('error', (msg) => {
	console.error(`[Server-Agent] Error: ${msg.message}`);
});

// ---- Connect ----

async function main() {
	console.log(`[Server-Agent] Starting — relay: ${RELAY_URL}, ollama: ${OLLAMA_URL}`);
	try {
		await relay.connect();
	} catch (err) {
		console.error('[Server-Agent] Failed to connect:', err.message);
		process.exit(1);
	}
}

main();

// Graceful shutdown
process.on('SIGINT', () => {
	console.log('\n[Server-Agent] Shutting down...');
	// Abort all active requests
	for (const [reqId, controller] of activeRequests) {
		controller.abort();
	}
	activeRequests.clear();
	relay.disconnect();
	process.exit(0);
});

process.on('SIGTERM', () => {
	relay.disconnect();
	process.exit(0);
});
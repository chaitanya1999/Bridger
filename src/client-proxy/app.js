/**
 * Client-Proxy App Factory
 *
 * Creates the Express app and RelayClient, wiring them together.
 * Exported for both production (index.js) and testing.
 *
 * Usage:
 *   const { createClientProxy } = require('./client-proxy/app');
 *   const proxy = await createClientProxy(relayUrl, { port, apiKey, clientId });
 *   proxy.start(); // starts HTTP server
 *   proxy.stop();  // shuts down
 */

const http = require('http');
const express = require('express');
const RelayClient = require('../relayClient');
const { toRelayRequest, createSSEResponse, createJSONResponse } = require('./openaiAdapter');

/**
 * @param {string} relayUrl  WebSocket URL of the relay server
 * @param {object} [opts]
 * @param {number} [opts.port=0]  HTTP listen port (0 = random)
 * @param {string} [opts.apiKey='dummy']
 * @param {string} [opts.clientId]  Auto-generated if omitted
 * @param {boolean} [opts.autoReconnect=true]
 * @returns {Promise<{ app, relay, server, port, start, stop }>}
 */
async function createClientProxy(relayUrl, opts = {}) {
	const port = opts.port || 0;
	const API_KEY = opts.apiKey || 'dummy';
	const CLIENT_ID = opts.clientId || `client-${Date.now()}`;
	const autoReconnect = opts.autoReconnect !== false;

	const app = express();
	app.use(express.json({ limit: '10mb' }));

	// ---- Auth Middleware ----
	function authMiddleware(req, res, next) {
		const provided = req.headers['authorization']?.replace(/^Bearer\s+/i, '') || '';
		if (provided !== API_KEY) {
			return res.status(401).json({
				error: { message: 'Invalid API key', type: 'auth_error', param: null, code: 'invalid_api_key' },
			});
		}
		next();
	}

	// ---- Relay Client ----
	const relay = new RelayClient(relayUrl, {
		role: 'LLM_CLIENT',
		clientId: CLIENT_ID,
		autoReconnect,
		logFilter: opts.logFilter,
	});

	const activeRequests = new Map(); // requestId → { onToken, onComplete, onError, timer }

	relay.on('registered', (msg) => {
		console.log(`[Client-Proxy] Registered as LLM-Client: ${msg.clientId}`);
	});

	relay.on('token', (msg) => {
		const h = activeRequests.get(msg.requestId);
		if (h) h.onToken(msg.requestId, msg.delta);
	});

	relay.on('complete', (msg) => {
		const h = activeRequests.get(msg.requestId);
		if (h) { h.onComplete(msg.requestId); cleanupRequest(msg.requestId); }
	});

	relay.on('error', (msg) => {
		const h = activeRequests.get(msg.requestId);
		if (h) { h.onError(msg.requestId, msg.message); cleanupRequest(msg.requestId); }
	});

	relay.on('cancel', (msg) => {
		const h = activeRequests.get(msg.requestId);
		if (h) { h.onError(msg.requestId, 'Request cancelled'); cleanupRequest(msg.requestId); }
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
	 */
	app.post('/v1/chat/completions', authMiddleware, async (req, res) => {
		try {
			const relayReq = toRelayRequest(req.body);
			const { model, messages, stream } = relayReq;
			const requestId = `req-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

			const handler = stream
				? createSSEResponse(res, model)
				: createJSONResponse(res, model);

			// Send headers immediately so the client knows the request was accepted
			res.writeHead(200, handler.headers);

			activeRequests.set(requestId, handler);
			relay.sendRequest(requestId, model, messages, stream);

			console.log(`[Client-Proxy] Sent REQUEST ${requestId} (model=${model}, stream=${stream})`);

			// Request timeout (5 minutes)
			const timer = setTimeout(() => {
				const h = activeRequests.get(requestId);
				if (h) { h.onError(requestId, 'Request timed out'); cleanupRequest(requestId); }
			}, 300_000);
			activeRequests.get(requestId).timer = timer;

			// If the client truly disconnects mid-request, send CANCEL
			req.on('close', () => {
				if (res.destroyed && activeRequests.has(requestId)) {
					console.log(`[Client-Proxy] Client disconnected, cancelling ${requestId}`);
					relay.sendCancel(requestId);
					cleanupRequest(requestId);
				}
			});

		} catch (err) {
			console.error('[Client-Proxy] Error handling /v1/chat/completions:', err.message);
			res.status(400).json({
				error: { message: err.message, type: 'invalid_request_error', param: null, code: null },
			});
		}
	});

	/**
	 * GET /v1/models
	 */
	app.get('/v1/models', authMiddleware, async (_req, res) => {
		if (!relay.registered) {
			return res.json({
				object: 'list',
				data: [
					{ id: 'llama3', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' },
					{ id: 'gemma4', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' },
					{ id: 'mistral', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' },
				],
			});
		}

		const requestId = `list-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

		try {
			const models = await fetchModelsViaRelay(requestId);
			res.json({
				object: 'list',
				data: models.map((m) => ({
					id: m.name,
					object: 'model',
					created: Math.floor(new Date(m.modified_at || Date.now()).getTime() / 1000),
					owned_by: 'ollama',
				})),
			});
		} catch (err) {
			console.error(`[Client-Proxy] Failed to fetch models via relay: ${err.message}`);
			res.json({
				object: 'list',
				data: [
					{ id: 'llama3', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' },
					{ id: 'gemma4', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' },
					{ id: 'mistral', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' },
				],
			});
		}
	});

	/**
	 * Fetch the model list from the server-agent via the relay.
	 */
	function fetchModelsViaRelay(requestId) {
		return new Promise((resolve, reject) => {
			let settled = false;

			const timeout = setTimeout(() => {
				if (settled) return;
				settled = true;
				relay.removeListener('models_list', onModelsList);
				relay.removeListener('error', onError);
				reject(new Error('Request timed out'));
			}, 15_000);

			function cleanup() {
				clearTimeout(timeout);
				relay.removeListener('models_list', onModelsList);
				relay.removeListener('error', onError);
			}

			function onModelsList(msg) {
				if (settled) return;
				if (msg.requestId === requestId) {
					settled = true;
					cleanup();
					resolve(msg.models || []);
				}
			}

			function onError(msg) {
				if (settled) return;
				// Match by requestId if present, otherwise match any error
				// that arrives during this request's window (handles old relays
				// that return error without requestId for unknown message types)
				if (!msg.requestId || msg.requestId === requestId) {
					settled = true;
					cleanup();
					reject(new Error(msg.message || 'Unknown error'));
				}
			}

			relay.on('models_list', onModelsList);
			relay.on('error', onError);
			relay.sendListModels(requestId);
		});
	}

	/**
	 * GET /health
	 */
	app.get('/health', (_req, res) => {
		res.json({ ok: true, relayConnected: relay.registered, clientId: CLIENT_ID });
	});

	// ---- Lifecycle ----

	let server = null;

	function start() {
		return new Promise((resolve) => {
			server = http.createServer(app);
			server.listen(port, () => {
				const actualPort = server.address().port;
				console.log(`[Client-Proxy] HTTP server listening on port ${actualPort}`);
				resolve(actualPort);
			});
		});
	}

	function stop() {
		relay.disconnect();
		if (server) {
			server.close();
			server = null;
		}
	}

	// Connect to relay
	await relay.connect();

	return { app, relay, server: () => server, port: () => server ? server.address().port : null, start, stop };
}

module.exports = { createClientProxy };
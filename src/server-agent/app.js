/**
 * Server-Agent App Factory
 *
 * Creates the RelayClient and OllamaClient, wiring them together.
 * Exported for both production (index.js) and testing.
 *
 * Usage:
 *   const { createServerAgent } = require('./server-agent/app');
 *   const agent = await createServerAgent(relayUrl, ollamaUrl, opts);
 *   agent.stop();
 */

const RelayClient = require('../relayClient');
const OllamaClient = require('./ollamaClient');

/**
 * @param {string} relayUrl  WebSocket URL of the relay server
 * @param {string} [ollamaUrl='http://localhost:11434']
 * @param {object} [opts]
 * @param {string} [opts.serverId]  Auto-generated if omitted
 * @param {string} [opts.secret]    Shared secret for relay auth
 * @param {boolean} [opts.autoReconnect=true]
 * @returns {Promise<{ relay, stop }>}
 */
async function createServerAgent(relayUrl, ollamaUrl, opts = {}) {
	const OLLAMA_URL = ollamaUrl || 'http://localhost:11434';
	const SERVER_ID = opts.serverId;
	const RELAY_SECRET = opts.secret;
	const autoReconnect = opts.autoReconnect !== false;

	const ollama = new OllamaClient(OLLAMA_URL);

	const relay = new RelayClient(relayUrl, {
		role: 'LLM_SERVER',
		serverId: SERVER_ID,
		secret: RELAY_SECRET,
		autoReconnect,
		logFilter: opts.logFilter,
	});

	// Track active requests so we can abort them on CANCEL
	const activeRequests = new Map(); // requestId → AbortController

	relay.on('registered', (msg) => {
		console.log(`[Server-Agent] Registered as LLM-Server: ${msg.serverId}`);
	});

	/**
	 * Handle an incoming REQUEST from a client via the relay.
	 */
	relay.on('request', async (msg) => {
		const { requestId, model, messages, stream = true } = msg;
		console.log(`[Server-Agent] Received REQUEST ${requestId} (model=${model}, stream=${stream})`);

		const controller = new AbortController();
		activeRequests.set(requestId, controller);

		try {
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

	/**
	 * Handle LIST_MODELS from the relay — fetch model list from Ollama.
	 */
	relay.on('list_models', async (msg) => {
		const { requestId } = msg;
		console.log(`[Server-Agent] Received LIST_MODELS ${requestId}`);
		try {
			const models = await ollama.listModels();
			relay.sendModelsList(requestId, models);
			console.log(`[Server-Agent] Sent MODELS_LIST ${requestId}: ${models.length} models`);
		} catch (err) {
			console.error(`[Server-Agent] Failed to list models: ${err.message}`);
			relay.sendError(requestId, `Failed to list models: ${err.message}`);
		}
	});

	relay.on('error', (msg) => {
		console.error(`[Server-Agent] Error: ${msg.message}`);
	});

	// Connect
	await relay.connect();

	function stop() {
		for (const [reqId, controller] of activeRequests) {
			controller.abort();
		}
		activeRequests.clear();
		relay.disconnect();
	}

	return { relay, stop };
}

module.exports = { createServerAgent };
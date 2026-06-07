/**
 * Real Ollama Integration Test
 *
 * Tests the full relay pipeline against a real local Ollama instance.
 *
 * Prerequisites:
 *   - Ollama must be running on http://localhost:11434
 *   - Model 'rafw007/qwen35-claude-coder:4b' must be pulled
 *
 * Usage:
 *   node test-real-ollama.js
 *
 * Environment variables:
 *   OLLAMA_URL   Ollama server URL (default: http://localhost:11434)
 *   MODEL        Model name (default: rafw007/qwen35-claude-coder:4b)
 */

const http = require('http');
const { attachWebSocketServer } = require('../src/websocketServer');
const RelayClient = require('../src/relayClient');
const { toRelayRequest, createSSEResponse, createJSONResponse } = require('../src/client-proxy/openaiAdapter');
const express = require('express');

const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434';
const MODEL = process.env.MODEL || 'rafw007/qwen35-claude-coder:4b';

// ---- Test utilities ----

let passed = 0;
let failed = 0;
let skipped = 0;

function test(name, fn) {
	return fn().then(() => {
		console.log(`  ✓ ${name}`);
		passed++;
	}).catch((err) => {
		if (err.message.includes('SKIP')) {
			console.log(`  ⚬ ${name} (skipped: ${err.message})`);
			skipped++;
		} else {
			console.log(`  ✗ ${name}: ${err.message}`);
			failed++;
		}
	});
}

function delay(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---- Helper: check if Ollama is running ----

async function checkOllama() {
	return new Promise((resolve) => {
		const url = new URL(OLLAMA_URL);
		const req = http.get(
			{ hostname: url.hostname, port: url.port, path: '/api/tags', timeout: 3000 },
			(res) => {
				let data = '';
				res.on('data', (chunk) => (data += chunk));
				res.on('end', () => {
					try {
						const result = JSON.parse(data);
						if (result.models) resolve(true);
						else resolve(false);
					} catch {
						resolve(false);
					}
				});
			}
		);
		req.on('error', () => resolve(false));
		req.on('timeout', () => { req.destroy(); resolve(false); });
	});
}

// ---- Start relay server ----

function startRelay() {
	return new Promise((resolve) => {
		const server = http.createServer((_req, res) => res.end('ok'));
		attachWebSocketServer(server);
		server.listen(0, () => resolve(server));
	});
}

// ---- Start server-agent (in-process, simplified) ----

function startServerAgent(relayUrl) {
	return new Promise((resolve, reject) => {
		const relay = new RelayClient(relayUrl, {
			role: 'LLM_SERVER',
			serverId: 'real-ollama-agent',
			autoReconnect: false,
		});

		relay.on('registered', () => {
			console.log('    [Test] Server-Agent registered');
		});

		relay.on('request', async (msg) => {
			const { requestId, model, messages, stream = true } = msg;

			try {
				const body = JSON.stringify({ model, messages, stream });
				const url = new URL(OLLAMA_URL);
				const options = {
					hostname: url.hostname,
					port: url.port,
					path: '/api/chat',
					method: 'POST',
					headers: {
						'content-type': 'application/json',
						'content-length': Buffer.byteLength(body),
					},
					timeout: 300_000,
				};

				const req = http.request(options, (res) => {
					let buffer = '';
					res.on('data', (chunk) => {
						buffer += chunk.toString();
						if (stream) {
							// Process lines as they arrive
							const lines = buffer.split('\n');
							buffer = lines.pop() || '';
							for (const line of lines) {
								if (!line.trim()) continue;
								try {
									const data = JSON.parse(line);
									if (data.message?.content) {
										relay.sendToken(requestId, { content: data.message.content });
									}
									if (data.done) {
										relay.sendComplete(requestId);
									}
								} catch { /* skip */ }
							}
						}
					});
					res.on('end', () => {
						if (!stream && buffer.trim()) {
							try {
								const data = JSON.parse(buffer);
								if (data.message?.content) {
									relay.sendToken(requestId, { content: data.message.content });
								}
								relay.sendComplete(requestId);
							} catch (err) {
								relay.sendError(requestId, 'Failed to parse Ollama response');
							}
						}
					});
				});

				req.on('error', (err) => relay.sendError(requestId, err.message));
				req.on('timeout', () => {
					req.destroy();
					relay.sendError(requestId, 'Ollama timeout');
				});
				req.write(body);
				req.end();

			} catch (err) {
				relay.sendError(requestId, err.message);
			}
		});

		relay.connect().then(() => resolve(relay)).catch(reject);
	});
}

// ---- Start client-proxy (in-process) ----

function startClientProxy(relayUrl) {
	return new Promise((resolve, reject) => {
		const relay = new RelayClient(relayUrl, {
			role: 'LLM_CLIENT',
			clientId: 'test-real-client',
			autoReconnect: false,
		});

		const activeRequests = new Map();

		relay.on('registered', () => {
			console.log('    [Test] Client-Proxy registered');
		});

		relay.on('token', (msg) => {
			const h = activeRequests.get(msg.requestId);
			if (h) h.onToken(msg.requestId, msg.delta);
		});

		relay.on('complete', (msg) => {
			const h = activeRequests.get(msg.requestId);
			if (h) { h.onComplete(msg.requestId); activeRequests.delete(msg.requestId); }
		});

		relay.on('error', (msg) => {
			const h = activeRequests.get(msg.requestId);
			if (h) { h.onError(msg.requestId, msg.message); activeRequests.delete(msg.requestId); }
		});

		const app = express();
		app.use(express.json());

		app.post('/v1/chat/completions', async (req, res) => {
			try {
				const relayReq = toRelayRequest(req.body);
				const { model, messages, stream } = relayReq;
				const requestId = `req-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

				let handler;
				if (stream) {
					handler = createSSEResponse(res, model);
					res.writeHead(200, handler.headers);
				} else {
					handler = createJSONResponse(res, model);
				}

				activeRequests.set(requestId, handler);
				relay.sendRequest(requestId, model, messages, stream);

				setTimeout(() => {
					const h = activeRequests.get(requestId);
					if (h) { h.onError(requestId, 'Timeout'); activeRequests.delete(requestId); }
				}, 120_000);

			} catch (err) {
				res.status(400).json({ error: { message: err.message } });
			}
		});

		relay.connect().then(() => {
			const server = http.createServer(app);
			server.listen(0, () => resolve({ server, relay }));
		}).catch(reject);
	});
}

// ---- HTTP helper ----

function httpRequest(url, options, body) {
	return new Promise((resolve, reject) => {
		const u = new URL(url);
		const opts = {
			hostname: u.hostname,
			port: u.port,
			path: u.pathname,
			method: options.method || 'GET',
			headers: options.headers || {},
			timeout: 120_000,
		};
		const client = http.request(opts, (res) => {
			let data = '';
			res.on('data', (chunk) => (data += chunk));
			res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: data }));
		});
		client.on('error', reject);
		client.on('timeout', () => { client.destroy(); reject(new Error('HTTP timeout')); });
		if (body) client.write(body);
		client.end();
	});
}

// ---- Main ----

async function runTests() {
	console.log('=== Real Ollama Integration Test ===\n');

	// Check if Ollama is running
	const ollamaRunning = await checkOllama();
	if (!ollamaRunning) {
		console.log(`  ⚠ Ollama not running at ${OLLAMA_URL}`);
		console.log('  Start Ollama and ensure it is accessible.\n');
	}

	// Start relay
	const relayServer = await startRelay();
	const relayPort = relayServer.address().port;
	const relayUrl = `ws://localhost:${relayPort}`;
	console.log(`  Relay server on port ${relayPort}`);

	// Start server-agent
	const serverAgent = await startServerAgent(relayUrl);
	await delay(100);

	// Start client-proxy
	const { server: proxyServer, relay: proxyRelay } = await startClientProxy(relayUrl);
	const proxyPort = proxyServer.address().port;
	console.log(`  Client-Proxy on port ${proxyPort}\n`);
	await delay(200);

	const apiUrl = `http://localhost:${proxyPort}`;

	// ---- Tests ----

	// Test 1: Health
	await test('Health endpoint', async () => {
		const res = await httpRequest(`${apiUrl}/health`, { method: 'GET' });
		const data = JSON.parse(res.body);
		if (!data.ok) throw new Error('Health check failed');
	});

	// Test 2: Non-streaming with Ollama (if running)
	await test(`Non-streaming with ${MODEL}`, async () => {
		if (!ollamaRunning) throw new Error('SKIP — Ollama not running');

		const res = await httpRequest(`${apiUrl}/v1/chat/completions`,
			{
				method: 'POST',
				headers: { 'content-type': 'application/json' },
			},
			JSON.stringify({
				model: MODEL,
				messages: [{ role: 'user', content: 'Reply with just the word "Hello" and nothing else.' }],
				stream: false,
			}),
		);

		const data = JSON.parse(res.body);
		if (data.object !== 'chat.completion') throw new Error('Expected chat.completion object');
		if (!data.choices?.[0]?.message?.content) throw new Error('Expected response content');
		console.log(`      Response: "${data.choices[0].message.content.slice(0, 100)}..."`);
	});

	// Test 3: Streaming with Ollama (if running)
	await test(`Streaming with ${MODEL}`, async () => {
		if (!ollamaRunning) throw new Error('SKIP — Ollama not running');

		const res = await httpRequest(`${apiUrl}/v1/chat/completions`,
			{
				method: 'POST',
				headers: { 'content-type': 'application/json' },
			},
			JSON.stringify({
				model: MODEL,
				messages: [{ role: 'user', content: 'Count from 1 to 3. Reply with just numbers.' }],
				stream: true,
			}),
		);

		if (res.status !== 200) throw new Error(`Expected 200, got ${res.status}`);
		if (!res.headers['content-type']?.includes('text/event-stream')) {
			throw new Error('Expected SSE content-type');
		}

		const lines = res.body.split('\n').filter(l => l.startsWith('data: '));
		const doneEvent = lines.find(l => l.trim() === 'data: [DONE]');
		if (!doneEvent) throw new Error('Expected [DONE] event');

		const tokenEvents = lines.filter(l => {
			const json = l.slice(6).trim();
			return json !== '[DONE]';
		});
		if (tokenEvents.length === 0) throw new Error('Expected at least one token event');

		// Verify structure of first event
		const first = JSON.parse(tokenEvents[0].slice(6));
		if (first.object !== 'chat.completion.chunk') throw new Error('Expected chunk object');
		if (!first.choices?.[0]) throw new Error('Expected choices');
		console.log(`      Received ${tokenEvents.length} token chunks`);
	});

	// Test 4: Model listing via Ollama
	await test(`List models from Ollama (via /api/tags)`, async () => {
		if (!ollamaRunning) throw new Error('SKIP — Ollama not running');

		// Direct call to Ollama to list models
		const url = new URL(OLLAMA_URL);
		const res = await new Promise((resolve, reject) => {
			const req = http.get(
				{ hostname: url.hostname, port: url.port, path: '/api/tags', timeout: 5000 },
				(r) => {
					let data = '';
					r.on('data', (c) => (data += c));
					r.on('end', () => resolve(JSON.parse(data)));
				}
			);
			req.on('error', reject);
			req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
		});

		if (!res.models || res.models.length === 0) throw new Error('No models found');
		const modelNames = res.models.map(m => m.name);
		const hasTarget = modelNames.some(n => n.includes(MODEL.split(':')[0]));
		console.log(`      Available models: ${modelNames.join(', ')}`);
		if (!hasTarget) {
			console.log(`      (note: ${MODEL} not found in model list, but test still passes)`);
		}
	});

	// ---- Summary ----
	console.log(`\n${passed + failed + skipped} tests — ${passed} passed, ${failed} failed, ${skipped} skipped`);

	// Cleanup
	proxyRelay.disconnect();
	serverAgent.disconnect();
	proxyServer.close();
	relayServer.close();

	process.exit(failed ? 1 : 0);
}

runTests().catch((err) => {
	console.error('Test runner error:', err);
	process.exit(1);
});
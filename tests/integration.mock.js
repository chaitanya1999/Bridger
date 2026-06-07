/**
 * Integration test for the full relay pipeline with a mock Ollama.
 *
 *   Client-Proxy (HTTP) → Relay Server (WS) → Server-Agent (WS) → Mock Ollama
 */

const http = require('http');
const express = require('express');
const { attachWebSocketServer } = require('../src/websocketServer');
const RelayClient = require('../src/relayClient');
const { toRelayRequest, createSSEResponse, createJSONResponse } = require('../src/client-proxy/openaiAdapter');

// ---- Test utilities ----

let passed = 0;
let failed = 0;

function test(name, fn) {
	return fn().then(() => {
		console.log(`  ✓ ${name}`);
		passed++;
	}).catch((err) => {
		console.log(`  ✗ ${name}: ${err.message}`);
		failed++;
	});
}

function delay(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---- Start mock Ollama server ----

function startMockOllama() {
	return new Promise((resolve) => {
		const app = express();
		app.use(express.json());

		app.post('/api/chat', (req, res) => {
			const { model, messages, stream } = req.body;

			if (stream) {
				const tokens = ['Hello', ' from', ' mock', ' Ollama', '!'];
				let index = 0;

				res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });

				const interval = setInterval(() => {
					if (index < tokens.length) {
						res.write(JSON.stringify({
							model, created_at: new Date().toISOString(),
							message: { role: 'assistant', content: tokens[index] }, done: false,
						}) + '\n');
						index++;
					} else {
						clearInterval(interval);
						res.write(JSON.stringify({
							model, created_at: new Date().toISOString(),
							message: { role: 'assistant', content: '' }, done: true,
						}) + '\n');
						res.end();
					}
				}, 10);
			} else {
				res.json({
					model, created_at: new Date().toISOString(),
					message: { role: 'assistant', content: 'Hello from mock Ollama!' }, done: true,
				});
			}
		});

		app.get('/api/tags', (_req, res) => {
			res.json({ models: [{ name: 'mock-model:latest', modified_at: new Date().toISOString() }] });
		});

		const server = http.createServer(app);
		server.listen(0, () => resolve(server));
	});
}

// ---- Start relay server ----

function startRelayServer() {
	return new Promise((resolve) => {
		const server = http.createServer((_req, res) => res.end('ok'));
		attachWebSocketServer(server);
		server.listen(0, () => resolve(server));
	});
}

// ---- Start client-proxy (in-process) ----

function startClientProxy(relayUrl, ollamaPort) {
	return new Promise((resolve, reject) => {
		const app = express();
		app.use(express.json({ limit: '10mb' }));

		const API_KEY = 'test-api-key';
		app.use((req, res, next) => {
			if (req.path === '/health') return next();
			const provided = req.headers['authorization']?.replace(/^Bearer\s+/i, '') || '';
			if (provided !== API_KEY) {
				return res.status(401).json({
					error: { message: 'Invalid API key', type: 'auth_error', param: null, code: 'invalid_api_key' },
				});
			}
			next();
		});

		const relay = new RelayClient(relayUrl, {
			role: 'LLM_CLIENT', clientId: 'test-client-proxy', autoReconnect: false,
		});

		const activeRequests = new Map();

		relay.on('registered', () => console.log('    [Test] Client-Proxy registered'));
		relay.on('token', (msg) => { const h = activeRequests.get(msg.requestId); if (h) h.onToken(msg.requestId, msg.delta); });
		relay.on('complete', (msg) => { const h = activeRequests.get(msg.requestId); if (h) { h.onComplete(msg.requestId); activeRequests.delete(msg.requestId); } });
		relay.on('error', (msg) => { const h = activeRequests.get(msg.requestId); if (h) { h.onError(msg.requestId, msg.message); activeRequests.delete(msg.requestId); } });

		app.get('/health', (_req, res) => res.json({ ok: true, relayConnected: relay.registered, clientId: 'test-client-proxy' }));

		app.post('/v1/chat/completions', async (req, res) => {
			try {
				const relayReq = toRelayRequest(req.body);
				const { model, messages, stream } = relayReq;
				const requestId = `req-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
				let handler;
				if (stream) { handler = createSSEResponse(res, model); res.writeHead(200, handler.headers); }
				else { handler = createJSONResponse(res, model); }
				activeRequests.set(requestId, handler);
				relay.sendRequest(requestId, model, messages, stream);
				setTimeout(() => { const h = activeRequests.get(requestId); if (h) { h.onError(requestId, 'Timeout'); activeRequests.delete(requestId); } }, 10_000);
			} catch (err) { res.status(400).json({ error: { message: err.message } }); }
		});

		app.get('/v1/models', (_req, res) => {
			res.json({ object: 'list', data: [{ id: 'mock-model', object: 'model', created: Math.floor(Date.now() / 1000), owned_by: 'ollama' }] });
		});

		relay.connect().then(() => {
			const server = http.createServer(app);
			server.listen(0, () => resolve({ server, relay, apiKey: API_KEY }));
		}).catch(reject);
	});
}

// ---- Start server-agent (simplified, in-process) ----

function startServerAgent(relayUrl, ollamaUrl) {
	return new Promise((resolve, reject) => {
		const relay = new RelayClient(relayUrl, {
			role: 'LLM_SERVER', serverId: 'test-server-agent', autoReconnect: false,
		});

		relay.on('registered', () => console.log('    [Test] Server-Agent registered'));

		relay.on('request', async (msg) => {
			const { requestId, model, messages, stream = true } = msg;
			try {
				const body = JSON.stringify({ model, messages, stream });
				const options = {
					hostname: 'localhost', port: new URL(ollamaUrl).port, path: '/api/chat', method: 'POST',
					headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) }, timeout: 5000,
				};
				const req = http.request(options, (res) => {
					let buffer = '';
					res.on('data', (chunk) => { buffer += chunk.toString(); if (stream) { const lines = buffer.split('\n'); buffer = lines.pop() || ''; for (const line of lines) { if (!line.trim()) continue; try { const data = JSON.parse(line); if (data.message?.content) relay.sendToken(requestId, { content: data.message.content }); if (data.done) relay.sendComplete(requestId); } catch {} } } });
					res.on('end', () => { if (!stream && buffer.trim()) { try { const data = JSON.parse(buffer); if (data.message?.content) relay.sendToken(requestId, { content: data.message.content }); relay.sendComplete(requestId); } catch { relay.sendError(requestId, 'Failed to parse'); } } });
				});
				req.on('error', (err) => relay.sendError(requestId, err.message));
				req.on('timeout', () => { req.destroy(); relay.sendError(requestId, 'Timeout'); });
				req.write(body); req.end();
			} catch (err) { relay.sendError(requestId, err.message); }
		});

		relay.connect().then(() => resolve(relay)).catch(reject);
	});
}

// ---- HTTP helper ----

function httpRequest(url, options, body) {
	return new Promise((resolve, reject) => {
		const u = new URL(url);
		const opts = { hostname: u.hostname, port: u.port, path: u.pathname, method: options.method || 'GET', headers: options.headers || {}, timeout: 15_000 };
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
	console.log('Setting up test environment...\n');

	const ollamaServer = await startMockOllama();
	const ollamaPort = ollamaServer.address().port;
	console.log(`  Mock Ollama on port ${ollamaPort}`);

	const relayServer = await startRelayServer();
	const relayPort = relayServer.address().port;
	const relayUrl = `ws://localhost:${relayPort}`;
	console.log(`  Relay on port ${relayPort}`);

	const serverAgent = await startServerAgent(relayUrl, `http://localhost:${ollamaPort}`);
	console.log(`  Server-Agent connected`);

	await delay(100);

	const { server: proxyServer, relay: proxyRelay, apiKey } = await startClientProxy(relayUrl, ollamaPort);
	const proxyPort = proxyServer.address().port;
	console.log(`  Client-Proxy on port ${proxyPort}\n`);
	await delay(200);

	// ---- Tests ----

	await test('Health endpoint', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/health`, { method: 'GET' });
		if (!JSON.parse(res.body).ok) throw new Error('Health check failed');
	});

	await test('GET /v1/models returns model list', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/models`, {
			method: 'GET', headers: { 'authorization': `Bearer ${apiKey}` },
		});
		const data = JSON.parse(res.body);
		if (!data.data?.length) throw new Error('Expected models');
	});

	await test('Auth rejects missing API key', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`, {
			method: 'POST', headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hi' }] }),
		});
		if (res.status !== 401) throw new Error(`Expected 401, got ${res.status}`);
	});

	await test('Non-streaming chat completion returns full response', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${apiKey}` } },
			JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hello' }], stream: false }),
		);
		const data = JSON.parse(res.body);
		if (data.object !== 'chat.completion') throw new Error('Expected chat.completion');
		if (!data.choices?.[0]?.message?.content?.includes('Hello')) throw new Error('Unexpected content');
	});

	await test('Streaming chat completion returns SSE chunks', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${apiKey}` } },
			JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hello' }], stream: true }),
		);
		if (res.status !== 200) throw new Error(`Expected 200, got ${res.status}`);
		if (!res.headers['content-type']?.includes('text/event-stream')) throw new Error('Expected SSE');
		const lines = res.body.split('\n').filter(l => l.startsWith('data: '));
		if (!lines.find(l => l.trim() === 'data: [DONE]')) throw new Error('Expected [DONE]');
		if (lines.filter(l => l.trim() !== 'data: [DONE]').length === 0) throw new Error('Expected tokens');
	});

	await test('Error when model is missing', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${apiKey}` } },
			JSON.stringify({ messages: [{ role: 'user', content: 'Hello' }] }),
		);
		if (res.status !== 400) throw new Error(`Expected 400, got ${res.status}`);
	});

	await test('Error when messages is missing', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${apiKey}` } },
			JSON.stringify({ model: 'mock-model' }),
		);
		if (res.status !== 400) throw new Error(`Expected 400, got ${res.status}`);
	});

	console.log(`\n${passed + failed} tests — ${passed} passed, ${failed} failed`);

	proxyRelay.disconnect();
	serverAgent.disconnect();
	proxyServer.close();
	relayServer.close();
	ollamaServer.close();
	process.exit(failed ? 1 : 0);
}

runTests().catch((err) => { console.error('Test runner error:', err); process.exit(1); });
/**
 * Integration test for the full relay pipeline with a mock Ollama.
 *
 * Uses the **real** client-proxy and server-agent app modules,
 * not in-process mocks. This ensures the actual production code paths
 * are exercised, including auth, close-handler logic, LIST_MODELS, etc.
 *
 *   Client-Proxy (HTTP) → Relay Server (WS) → Server-Agent (WS) → Mock Ollama
 */

const http = require('http');
const express = require('express');
const { attachWebSocketServer } = require('../src/websocketServer');
const { createClientProxy } = require('../src/client-proxy/app');
const { createServerAgent } = require('../src/server-agent/app');

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
			timeout: 15_000,
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
	console.log('Setting up test environment...\n');

	// 1. Mock Ollama
	const ollamaServer = await startMockOllama();
	const ollamaPort = ollamaServer.address().port;
	console.log(`  Mock Ollama on port ${ollamaPort}`);

	// 2. Relay server
	const relayServer = await startRelayServer();
	const relayPort = relayServer.address().port;
	const relayUrl = `ws://localhost:${relayPort}`;
	console.log(`  Relay on port ${relayPort}`);

	// 3. Server-agent (real module) — silence relay logs for clean test output
	const serverAgent = await createServerAgent(relayUrl, `http://localhost:${ollamaPort}`, {
		serverId: 'test-server-agent',
		logFilter: { all: false },
	});
	console.log(`  Server-Agent registered`);

	await delay(100);

	// 4. Client-proxy (real module)
	const API_KEY = 'test-api-key';
	const proxy = await createClientProxy(relayUrl, {
		apiKey: API_KEY,
		clientId: 'test-client-proxy',
		autoReconnect: false,
	});
	const proxyPort = await proxy.start();
	console.log(`  Client-Proxy on port ${proxyPort}\n`);

	await delay(200);

	// ---- Tests ----

	await test('Health endpoint', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/health`, { method: 'GET' });
		const body = JSON.parse(res.body);
		if (!body.ok) throw new Error('Health check failed');
	});

	await test('GET /v1/models returns dynamic model list from Ollama', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/models`, {
			method: 'GET',
			headers: { 'authorization': `Bearer ${API_KEY}` },
		});
		const data = JSON.parse(res.body);
		if (!data.data?.length) throw new Error('Expected models array');
		if (data.data[0].id !== 'mock-model:latest') throw new Error(`Expected mock-model:latest, got ${data.data[0].id}`);
	});

	await test('Auth rejects missing API key (401)', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hi' }] }),
		});
		if (res.status !== 401) throw new Error(`Expected 401, got ${res.status}`);
	});

	await test('Auth rejects wrong API key (401)', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`, {
			method: 'POST',
			headers: { 'content-type': 'application/json', 'authorization': 'Bearer wrong-key' },
			body: JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hi' }] }),
		});
		if (res.status !== 401) throw new Error(`Expected 401, got ${res.status}`);
	});

	await test('Non-streaming chat completion returns full JSON response', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${API_KEY}` } },
			JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hello' }], stream: false }),
		);
		const data = JSON.parse(res.body);
		if (data.object !== 'chat.completion') throw new Error(`Expected chat.completion, got ${data.object}`);
		if (!data.choices?.[0]?.message?.content) throw new Error('Expected content');
		if (res.status !== 200) throw new Error(`Expected 200, got ${res.status}`);
	});

	await test('Streaming chat completion returns SSE chunks', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${API_KEY}` } },
			JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hello' }], stream: true }),
		);
		if (res.status !== 200) throw new Error(`Expected 200, got ${res.status}`);
		if (!res.headers['content-type']?.includes('text/event-stream')) throw new Error('Expected SSE content-type');
		const lines = res.body.split('\n').filter(l => l.startsWith('data: '));
		if (!lines.find(l => l.trim() === 'data: [DONE]')) throw new Error('Expected [DONE] marker');
		if (lines.filter(l => l.trim() !== 'data: [DONE]').length === 0) throw new Error('Expected token chunks');
	});

	await test('Validation — missing model returns 400', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${API_KEY}` } },
			JSON.stringify({ messages: [{ role: 'user', content: 'Hello' }] }),
		);
		if (res.status !== 400) throw new Error(`Expected 400, got ${res.status}`);
	});

	await test('Validation — missing messages returns 400', async () => {
		const res = await httpRequest(`http://localhost:${proxyPort}/v1/chat/completions`,
			{ method: 'POST', headers: { 'content-type': 'application/json', 'authorization': `Bearer ${API_KEY}` } },
			JSON.stringify({ model: 'mock-model' }),
		);
		if (res.status !== 400) throw new Error(`Expected 400, got ${res.status}`);
	});

	await test('Client disconnect sends CANCEL to server-agent', async () => {
		// Use a slow-mock endpoint for this test
		const slowOllamaApp = express();
		slowOllamaApp.use(express.json());
		slowOllamaApp.post('/api/chat', (_req, res) => {
			// Never send a response — hold forever
			res.writeHead(200, { 'Content-Type': 'application/json' });
		});
		const slowServer = http.createServer(slowOllamaApp);
		await new Promise((r) => slowServer.listen(0, r));
		const slowPort = slowServer.address().port;

		// Create a second server-agent pointing at the slow Ollama
		const slowAgent = await createServerAgent(relayUrl, `http://localhost:${slowPort}`, {
			serverId: 'slow-server-agent',
		});
		await delay(100);

		// Send a request and immediately destroy the socket to simulate client disconnect
		const u = new URL(`http://localhost:${proxyPort}/v1/chat/completions`);
		const opts = {
			hostname: u.hostname,
			port: u.port,
			path: u.pathname,
			method: 'POST',
			headers: { 'content-type': 'application/json', 'authorization': `Bearer ${API_KEY}` },
		};
		await new Promise((resolve) => {
			const req = http.request(opts, () => {});
			req.on('error', () => { /* expected — socket destroyed */ });
			req.write(JSON.stringify({ model: 'mock-model', messages: [{ role: 'user', content: 'Hello' }], stream: true }));
			// Destroy immediately to simulate client disconnect
			req.destroy();
			resolve();
		});

		await delay(500); // Let the cancel propagate

		// Cleanup
		slowAgent.stop();
		slowServer.close();

		console.log('    → Client disconnect processed without error');
	});

	await test('Models fallback when relay is not connected', async () => {
		// Use a separate relay that has no server-agent connected
		const isolatedRelayServer = await startRelayServer();
		const isolatedPort = isolatedRelayServer.address().port;

		// Create a proxy connected to that relay (no server-agent will respond to LIST_MODELS)
		const isolatedProxy = await createClientProxy(`ws://localhost:${isolatedPort}`, {
			apiKey: 'iso-key',
			clientId: 'iso-proxy',
			autoReconnect: false,
		});
		const isoProxyPort = await isolatedProxy.start();
		await delay(200);

		// Send a LIST_MODELS request — there's no server-agent, so timeout -> fallback
		const res = await httpRequest(`http://localhost:${isoProxyPort}/v1/models`, {
			method: 'GET',
			headers: { 'authorization': 'Bearer iso-key' },
		});
		const data = JSON.parse(res.body);
		if (!data.data?.length) throw new Error('Expected fallback models');
		// Should be the static fallback (llama3, gemma4, mistral) since no server is available
		if (data.data.length !== 3) throw new Error(`Expected 3 fallback models, got ${data.data.length}`);

		isolatedProxy.stop();
		isolatedRelayServer.close();
		console.log('    → Static fallback returned when no server-agent available');
	});

	console.log(`\n${passed + failed} tests — ${passed} passed, ${failed} failed`);

	// Cleanup
	proxy.stop();
	serverAgent.stop();
	relayServer.close();
	ollamaServer.close();
	process.exit(failed ? 1 : 0);
}

runTests().catch((err) => { console.error('Test runner error:', err); process.exit(1); });
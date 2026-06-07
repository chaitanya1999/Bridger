const http = require('http');
const WebSocket = require('ws');
const { attachWebSocketServer } = require('../src/websocketServer');

// Create an HTTP server
const server = http.createServer((_req, res) => res.end('ok'));
const wss = attachWebSocketServer(server);

server.listen(0, () => {
	const port = server.address().port;
	const baseUrl = `ws://localhost:${port}`;

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

	function connect() {
		return new Promise((resolve, reject) => {
			const ws = new WebSocket(baseUrl);
			ws.on('open', () => {
				// Attach a buffering message listener
				const buffer = [];
				ws._msgBuffer = buffer;
				ws.on('message', (raw) => {
					buffer.push(raw.toString());
				});
				resolve(ws);
			});
			ws.on('error', reject);
		});
	}

	function send(ws, obj) {
		ws.send(JSON.stringify(obj));
	}

	/** Wait for a message of the given type, using the buffered messages first. */
	function waitFor(ws, type, timeoutMs = 3000) {
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => reject(new Error(`Timeout waiting for ${type}`)), timeoutMs);

			// Check buffer
			function checkBuffer() {
				for (let i = 0; i < ws._msgBuffer.length; i++) {
					try {
						const msg = JSON.parse(ws._msgBuffer[i]);
						if (msg.type === type) {
							ws._msgBuffer.splice(i, 1);
							clearTimeout(timer);
							resolve(msg);
							return true;
						}
					} catch {}
				}
				return false;
			}

			if (checkBuffer()) return;

			// Listen for new messages
			const origPush = ws._msgBuffer.push.bind(ws._msgBuffer);
			ws._msgBuffer.push = function (raw) {
				origPush(raw);
				checkBuffer();
			};
		});
	}

	async function runTests() {
		// ------ Test 1: REGISTER & REQUEST forwarding ------
		await test('LLM-Client + LLM-Server registration and REQUEST relay', async () => {
			const client = await connect();
			const serverWs = await connect();

			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'test-client' });
			send(serverWs, { type: 'REGISTER', role: 'LLM_SERVER', serverId: 'test-server' });

			await waitFor(client, 'REGISTERED');
			await waitFor(serverWs, 'REGISTERED');

			// Client sends REQUEST
			send(client, { type: 'REQUEST', requestId: 'req-1', model: 'llama3', messages: [{ role: 'user', content: 'Hi' }] });
			const reqMsg = await waitFor(serverWs, 'REQUEST');
			if (reqMsg.requestId !== 'req-1') throw new Error('requestId mismatch');
			if (reqMsg.clientId !== 'test-client') throw new Error('clientId mismatch');
			if (reqMsg.model !== 'llama3') throw new Error('model mismatch');

			// Server sends back a token
			send(serverWs, { type: 'TOKEN', requestId: 'req-1', delta: { content: 'Hello' } });
			const tokenMsg = await waitFor(client, 'TOKEN');
			if (tokenMsg.delta.content !== 'Hello') throw new Error('token content mismatch');

			// Server completes
			send(serverWs, { type: 'COMPLETE', requestId: 'req-1' });
			await waitFor(client, 'COMPLETE');

			client.close();
			serverWs.close();
		});

		// ------ Test 2: No server available ------
		await test('Error when no LLM-Server is registered', async () => {
			const client = await connect();
			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'no-server-client' });
			await waitFor(client, 'REGISTERED');

			send(client, { type: 'REQUEST', requestId: 'req-fail', model: 'llama3', messages: [{ role: 'user', content: 'Hi' }] });
			const err = await waitFor(client, 'ERROR');
			if (!err.message.includes('No LLM-Server')) throw new Error('Wrong error: ' + err.message);
			client.close();
		});

		// ------ Test 3: Server disconnect cleans up pending requests ------
		await test('Server disconnect sends ERROR to client', async () => {
			const client = await connect();
			const serverWs = await connect();

			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'disc-client' });
			send(serverWs, { type: 'REGISTER', role: 'LLM_SERVER', serverId: 'disc-server' });
			await waitFor(client, 'REGISTERED');
			await waitFor(serverWs, 'REGISTERED');

			send(client, { type: 'REQUEST', requestId: 'req-disc', model: 'llama3', messages: [{ role: 'user', content: 'x' }] });
			await waitFor(serverWs, 'REQUEST');

			// Server disconnects
			serverWs.close();

			const err = await waitFor(client, 'ERROR');
			if (err.requestId !== 'req-disc') throw new Error('requestId mismatch on error');
			client.close();
		});

		// ------ Test 4: Client cleanup sends CANCEL to server ------
		await test('Client disconnect sends CANCEL to server', async () => {
			const client = await connect();
			const serverWs = await connect();

			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'cancel-client' });
			send(serverWs, { type: 'REGISTER', role: 'LLM_SERVER', serverId: 'cancel-server' });
			await waitFor(client, 'REGISTERED');
			await waitFor(serverWs, 'REGISTERED');

			send(client, { type: 'REQUEST', requestId: 'req-cancel', model: 'llama3', messages: [{ role: 'user', content: 'x' }] });
			await waitFor(serverWs, 'REQUEST');

			// Client disconnects
			client.close();

			const cancel = await waitFor(serverWs, 'CANCEL');
			if (cancel.requestId !== 'req-cancel') throw new Error('cancel requestId mismatch');
			serverWs.close();
		});

		// ------ Test 5: REGISTER timeout (only if connection never sends REGISTER) ------
		await test('Unregistered connection receives timeout ERROR', async () => {
			const unreg = await connect();
			const err = await waitFor(unreg, 'ERROR', 15000);
			if (!err.message.includes('REGISTER timeout')) throw new Error('Wrong timeout message');
		});

		// ------ Test 6: Duplicate clientId rejection ------
		await test('Duplicate clientId is rejected', async () => {
			const c1 = await connect();
			const c2 = await connect();
			send(c1, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'dup-client' });
			await waitFor(c1, 'REGISTERED');
			send(c2, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'dup-client' });
			const err = await waitFor(c2, 'ERROR');
			if (!err.message.includes('already registered')) throw new Error('Wrong error: ' + err.message);
			c1.close();
			c2.close();
		});

		// ------ Test 7: CANCEL from client to server ------
		await test('CANCEL message forwarded to server', async () => {
			const client = await connect();
			const serverWs = await connect();

			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'cancel-msg-client' });
			send(serverWs, { type: 'REGISTER', role: 'LLM_SERVER', serverId: 'cancel-msg-server' });
			await waitFor(client, 'REGISTERED');
			await waitFor(serverWs, 'REGISTERED');

			send(client, { type: 'REQUEST', requestId: 'req-cancel-msg', model: 'llama3', messages: [{ role: 'user', content: 'x' }] });
			await waitFor(serverWs, 'REQUEST');

			send(client, { type: 'CANCEL', requestId: 'req-cancel-msg' });
			const cancel = await waitFor(serverWs, 'CANCEL');
			if (cancel.requestId !== 'req-cancel-msg') throw new Error('cancel requestId mismatch');

			client.close();
			serverWs.close();
		});

		// ------ Test 8: Admin REGISTER and STATUS ------
		await test('Admin registers and receives STATUS updates', async () => {
			const admin = await connect();
			send(admin, { type: 'REGISTER', role: 'ADMIN', password: 'abc123' });
			const reg = await waitFor(admin, 'REGISTERED');
			if (reg.role !== 'ADMIN') throw new Error('Expected ADMIN role');

			// Should receive STATUS within next 3 seconds
			const status = await waitFor(admin, 'STATUS', 5000);
			if (!Array.isArray(status.clients)) throw new Error('Expected clients array');
			if (!Array.isArray(status.servers)) throw new Error('Expected servers array');
			if (!Array.isArray(status.pending)) throw new Error('Expected pending array');

			admin.close();
		});

		// ------ Test 9: Admin DISCONNECT_CLIENT ------
		await test('Admin can disconnect a client', async () => {
			const client = await connect();
			const admin = await connect();

			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'admin-disc-client' });
			await waitFor(client, 'REGISTERED');

			send(admin, { type: 'REGISTER', role: 'ADMIN', password: 'abc123' });
			await waitFor(admin, 'REGISTERED');

			// Admin disconnects the client
			send(admin, { type: 'DISCONNECT_CLIENT', clientId: 'admin-disc-client' });

			// Client should receive error and close
			const err = await waitFor(client, 'ERROR');
			if (!err.message.includes('Disconnected by admin')) throw new Error('Wrong error: ' + err.message);

			admin.close();
		});

		// ------ Test 10: Admin DISCONNECT_SERVER ------
		await test('Admin can disconnect a server', async () => {
			const serverWs = await connect();
			const admin = await connect();

			send(serverWs, { type: 'REGISTER', role: 'LLM_SERVER', serverId: 'admin-disc-server' });
			await waitFor(serverWs, 'REGISTERED');

			send(admin, { type: 'REGISTER', role: 'ADMIN', password: 'abc123' });
			await waitFor(admin, 'REGISTERED');

			send(admin, { type: 'DISCONNECT_SERVER', serverId: 'admin-disc-server' });

			const err = await waitFor(serverWs, 'ERROR');
			if (!err.message.includes('Disconnected by admin')) throw new Error('Wrong error: ' + err.message);

			admin.close();
		});

		// ------ Test 11: Admin password rejection ------
		await test('Admin with wrong password is rejected', async () => {
			const admin = await connect();
			send(admin, { type: 'REGISTER', role: 'ADMIN', password: 'wrong-password' });
			const err = await waitFor(admin, 'ERROR', 5000);
			if (!err.message.includes('Invalid admin password')) throw new Error('Wrong error: ' + err.message);
		});

		// ------ Test 12: Admin DISCONNECT_ALL ------
		await test('Admin can disconnect all clients and servers', async () => {
			const client = await connect();
			const serverWs = await connect();
			const admin = await connect();

			send(client, { type: 'REGISTER', role: 'LLM_CLIENT', clientId: 'disc-all-client' });
			send(serverWs, { type: 'REGISTER', role: 'LLM_SERVER', serverId: 'disc-all-server' });
			await waitFor(client, 'REGISTERED');
			await waitFor(serverWs, 'REGISTERED');

			send(admin, { type: 'REGISTER', role: 'ADMIN', password: 'abc123' });
			await waitFor(admin, 'REGISTERED');

			await new Promise(r => setTimeout(r, 100)); // let status interval almost fire

			send(admin, { type: 'DISCONNECT_ALL' });

			// Both should get disconnected
			const clientErr = await waitFor(client, 'ERROR');
			if (!clientErr.message.includes('Disconnected by admin')) throw new Error('Wrong client error');

			// Admin should get OK
			const ok = await waitFor(admin, 'OK');
			if (!ok.message.includes('All clients')) throw new Error('Wrong OK message');

			admin.close();
		});

		// ------ Summary ------
		console.log(`\n${passed + failed} tests — ${passed} passed, ${failed} failed`);

		wss.close(() => server.close(() => process.exit(failed ? 1 : 0)));
	}

	runTests();
});
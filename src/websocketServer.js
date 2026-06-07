const WebSocket = require('ws');

const PING_INTERVAL_MS = 25 * 1000;		// Send ping every 25s
const PONG_TIMEOUT_MS = 30 * 1000;		 // Close after no pong for 30s
const REGISTER_TIMEOUT_MS = 10 * 1000;	 // Close if REGISTER not received within 10s
const ADMIN_UPDATE_INTERVAL_MS = 2000;	 // Status broadcast to admins every 2s
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'abc123';

/**
 * Attaches a WebSocket server to an existing HTTP server.
 *
 * The relay supports two client roles:
 *   - LLM_CLIENT: sends REQUEST (prompts) to be forwarded to an LLM-Server.
 *   - LLM_SERVER: receives REQUESTs, processes them (e.g. via Ollama), and
 *                 streams back TOKEN / COMPLETE / ERROR messages.
 *   - ADMIN:     monitors relay state and can disconnect clients/servers.
 *
 * Messages use a JSON protocol:
 *   REGISTER           → { type, role, clientId/serverId, secret?, password? }
 *   REQUEST            → { type, requestId, model, messages, stream }
 *   TOKEN              → { type, requestId, delta }
 *   COMPLETE           → { type, requestId }
 *   ERROR              → { type, requestId, message }
 *   CANCEL             → { type, requestId }
 *   DISCONNECT_CLIENT  → { type, clientId }       (ADMIN only)
 *   DISCONNECT_SERVER  → { type, serverId }       (ADMIN only)
 *   DISCONNECT_ALL     → { type }                 (ADMIN only)
 *   STATUS             → { type, clients, servers, pending }  (sent to ADMIN)
 *   LOG                → { type, level, message } (sent to ADMIN)
 *
 * @param {import('http').Server} httpServer
 * @returns {WebSocket.Server}
 */
function attachWebSocketServer(httpServer) {
	const wss = new WebSocket.Server({ server: httpServer });

	// ---- Relay state ----
	const allConnections = new Set();			 // All WS connections (for heartbeat)
	const llmClients = new Map();				 // clientId → WebSocket
	const llmServers = new Map();				 // serverId → WebSocket
	const adminConnections = new Set();			 // Admin WebSocket connections
	const pendingRequests = new Map();			 // requestId → { clientId, serverId, createdAt }
	let serverSelectionIndex = 0;				 // round‑robin index

	// ---- Event log helpers ----

	/** Broadcast a log event to all connected admins. */
	function adminLog(level, message) {
		const msg = { type: 'LOG', level, message, timestamp: Date.now() };
		for (const admin of adminConnections) {
			sendJSON(admin, msg);
		}
	}

	/** Broadcast a status snapshot to all connected admins. */
	function broadcastStatus() {
		const clients = [...llmClients.keys()].map((id) => ({
			id,
			connectedAt: llmClients.get(id)._connectedAt || 0,
		}));
		const servers = [...llmServers.keys()].map((id) => ({
			id,
			connectedAt: llmServers.get(id)._connectedAt || 0,
		}));
		const pending = [...pendingRequests.entries()].map(([reqId, req]) => ({
			requestId: reqId,
			clientId: req.clientId,
			serverId: req.serverId,
			age: Date.now() - req.createdAt,
		}));

		const msg = {
			type: 'STATUS',
			clients,
			servers,
			pending,
			timestamp: Date.now(),
		};

		for (const admin of adminConnections) {
			sendJSON(admin, msg);
		}
	}

	// ---- Helpers ----

	function sendJSON(ws, obj) {
		if (ws.readyState === WebSocket.OPEN) {
			ws.send(JSON.stringify(obj));
		}
	}

	/** Pick the next available LLM-Server (round‑robin). */
	function pickServer() {
		const keys = [...llmServers.keys()];
		if (keys.length === 0) return null;
		const idx = serverSelectionIndex % keys.length;
		serverSelectionIndex = (serverSelectionIndex + 1) % keys.length;
		return { serverId: keys[idx], ws: llmServers.get(keys[idx]) };
	}

	/** Remove a client and cancel its pending requests. */
	function cleanupClient(clientId) {
		llmClients.delete(clientId);
		for (const [reqId, req] of pendingRequests) {
			if (req.clientId === clientId) {
				pendingRequests.delete(reqId);
				const serverWs = llmServers.get(req.serverId);
				if (serverWs) sendJSON(serverWs, { type: 'CANCEL', requestId: reqId });
			}
		}
		adminLog('info', `LLM-Client disconnected: ${clientId}`);
	}

	/** Remove a server and notify its clients that their requests failed. */
	function cleanupServer(serverId) {
		llmServers.delete(serverId);
		for (const [reqId, req] of pendingRequests) {
			if (req.serverId === serverId) {
				pendingRequests.delete(reqId);
				const clientWs = llmClients.get(req.clientId);
				if (clientWs) {
					sendJSON(clientWs, { type: 'ERROR', requestId: reqId, message: 'LLM-Server disconnected' });
				}
			}
		}
		adminLog('warn', `LLM-Server disconnected: ${serverId}`);
	}

	/** Remove connection from all tracking structures. */
	function removeConnection(ws) {
		allConnections.delete(ws);
		if (ws._pongTimeout) {
			clearTimeout(ws._pongTimeout);
			ws._pongTimeout = null;
		}
		if (ws._role === 'LLM_CLIENT' && ws._clientId) {
			cleanupClient(ws._clientId);
		} else if (ws._role === 'LLM_SERVER' && ws._serverId) {
			cleanupServer(ws._serverId);
		} else if (ws._role === 'ADMIN') {
			adminConnections.delete(ws);
			adminLog('info', 'Admin disconnected');
		}
	}

	// ---- Connection handler ----

	wss.on('connection', (ws) => {
		allConnections.add(ws);
		ws.isAlive = true;

		// Expect a REGISTER message within a short window
		const registerTimeout = setTimeout(() => {
			sendJSON(ws, { type: 'ERROR', message: 'REGISTER timeout — closing' });
			ws.close(4001, 'REGISTER timeout');
		}, REGISTER_TIMEOUT_MS);

		ws.on('message', (raw) => {
			let msg;
			try {
				msg = JSON.parse(raw.toString());
			} catch {
				sendJSON(ws, { type: 'ERROR', message: 'Invalid JSON' });
				return;
			}

			switch (msg.type) {

				case 'REGISTER': {
					clearTimeout(registerTimeout);

					if (msg.role === 'LLM_CLIENT') {
						if (!msg.clientId) {
							sendJSON(ws, { type: 'ERROR', message: 'clientId is required for LLM_CLIENT' });
							return;
						}
						if (llmClients.has(msg.clientId)) {
							sendJSON(ws, { type: 'ERROR', message: `clientId "${msg.clientId}" already registered` });
							return;
						}
						llmClients.set(msg.clientId, ws);
						ws._role = 'LLM_CLIENT';
						ws._clientId = msg.clientId;
						ws._connectedAt = Date.now();
						sendJSON(ws, { type: 'REGISTERED', role: 'LLM_CLIENT', clientId: msg.clientId });
						console.log(`[Relay] LLM-Client registered: ${msg.clientId}`);
						adminLog('info', `LLM-Client registered: ${msg.clientId}`);
					} else if (msg.role === 'LLM_SERVER') {
						// Optional shared-secret authentication
						if (process.env.RELAY_SECRET && msg.secret !== process.env.RELAY_SECRET) {
							sendJSON(ws, { type: 'ERROR', message: 'Invalid secret' });
							ws.close(4003, 'Unauthorized');
							return;
						}
						const serverId = msg.serverId || `server-${Date.now()}`;
						if (llmServers.has(serverId)) {
							sendJSON(ws, { type: 'ERROR', message: `serverId "${serverId}" already registered` });
							return;
						}
						llmServers.set(serverId, ws);
						ws._role = 'LLM_SERVER';
						ws._serverId = serverId;
						ws._connectedAt = Date.now();
						sendJSON(ws, { type: 'REGISTERED', role: 'LLM_SERVER', serverId });
						console.log(`[Relay] LLM-Server registered: ${serverId}`);
						adminLog('info', `LLM-Server registered: ${serverId}`);
					} else if (msg.role === 'ADMIN') {
						// Password check
						if (msg.password !== ADMIN_PASSWORD) {
							sendJSON(ws, { type: 'ERROR', message: 'Invalid admin password' });
							ws.close(4003, 'Unauthorized');
							return;
						}
						adminConnections.add(ws);
						ws._role = 'ADMIN';
						sendJSON(ws, { type: 'REGISTERED', role: 'ADMIN' });
						console.log('[Relay] Admin connected');
						adminLog('info', 'Admin connected');
					} else {
						sendJSON(ws, { type: 'ERROR', message: `Unknown role: ${msg.role}` });
					}
					break;
				}

				case 'REQUEST': {
					if (ws._role !== 'LLM_CLIENT') {
						sendJSON(ws, { type: 'ERROR', message: 'Only LLM-Clients may send REQUEST' });
						return;
					}
					const { requestId, model, messages, stream = true } = msg;
					if (!requestId) {
						sendJSON(ws, { type: 'ERROR', message: 'requestId is required' });
						return;
					}
					if (!messages || !Array.isArray(messages)) {
						sendJSON(ws, { type: 'ERROR', requestId, message: 'messages array is required' });
						return;
					}

					const server = pickServer();
					if (!server) {
						sendJSON(ws, { type: 'ERROR', requestId, message: 'No LLM-Server available' });
						return;
					}

					pendingRequests.set(requestId, {
						clientId: ws._clientId,
						serverId: server.serverId,
						createdAt: Date.now(),
					});

					sendJSON(server.ws, {
						type: 'REQUEST',
						requestId,
						clientId: ws._clientId,
						model,
						messages,
						stream,
					});
					adminLog('info', `REQUEST ${requestId}: ${ws._clientId} → ${server.serverId} (model: ${model})`);
					break;
				}

				case 'CANCEL': {
					const { requestId } = msg;
					if (!requestId) return;
					const req = pendingRequests.get(requestId);
					if (!req) return;
					pendingRequests.delete(requestId);

					if (ws._role === 'LLM_CLIENT') {
						const serverWs = llmServers.get(req.serverId);
						if (serverWs) sendJSON(serverWs, { type: 'CANCEL', requestId });
						adminLog('info', `CANCEL ${requestId}: client cancelled`);
					} else if (ws._role === 'LLM_SERVER') {
						const clientWs = llmClients.get(req.clientId);
						if (clientWs) sendJSON(clientWs, { type: 'CANCEL', requestId });
						adminLog('info', `CANCEL ${requestId}: server cancelled`);
					}
					break;
				}

				case 'TOKEN':
				case 'COMPLETE':
				case 'ERROR': {
					if (ws._role !== 'LLM_SERVER') {
						sendJSON(ws, { type: 'ERROR', message: `Only LLM-Servers may send ${msg.type}` });
						return;
					}
					const { requestId } = msg;
					const req = pendingRequests.get(requestId);
					if (!req) {
						// Request may have already been cancelled or completed — silently drop
						return;
					}
					const clientWs = llmClients.get(req.clientId);
					if (clientWs) {
						sendJSON(clientWs, msg);
					}
					if (msg.type === 'COMPLETE' || msg.type === 'ERROR') {
						pendingRequests.delete(requestId);
						if (msg.type === 'COMPLETE') {
							adminLog('info', `COMPLETE ${requestId}`);
						} else {
							adminLog('error', `ERROR ${requestId}: ${msg.message}`);
						}
					}
					break;
				}

				// ---- Admin commands ----

				case 'DISCONNECT_CLIENT': {
					if (ws._role !== 'ADMIN') {
						sendJSON(ws, { type: 'ERROR', message: 'Only admins may disconnect clients' });
						return;
					}
					const { clientId } = msg;
					if (!clientId) {
						sendJSON(ws, { type: 'ERROR', message: 'clientId is required' });
						return;
					}
					const clientWs = llmClients.get(clientId);
					if (clientWs) {
						sendJSON(clientWs, { type: 'ERROR', message: 'Disconnected by admin' });
						clientWs.close(4002, 'Disconnected by admin');
						// cleanupClient() is called via removeConnection() on close event
						adminLog('warn', `Admin disconnected client: ${clientId}`);
					} else {
						sendJSON(ws, { type: 'ERROR', message: `Client "${clientId}" not found` });
					}
					break;
				}

				case 'DISCONNECT_SERVER': {
					if (ws._role !== 'ADMIN') {
						sendJSON(ws, { type: 'ERROR', message: 'Only admins may disconnect servers' });
						return;
					}
					const { serverId } = msg;
					if (!serverId) {
						sendJSON(ws, { type: 'ERROR', message: 'serverId is required' });
						return;
					}
					const serverWs = llmServers.get(serverId);
					if (serverWs) {
						sendJSON(serverWs, { type: 'ERROR', message: 'Disconnected by admin' });
						serverWs.close(4002, 'Disconnected by admin');
						// cleanupServer() is called via removeConnection() on close event
						adminLog('warn', `Admin disconnected server: ${serverId}`);
					} else {
						sendJSON(ws, { type: 'ERROR', message: `Server "${serverId}" not found` });
					}
					break;
				}

				case 'DISCONNECT_ALL': {
					if (ws._role !== 'ADMIN') {
						sendJSON(ws, { type: 'ERROR', message: 'Only admins may disconnect all' });
						return;
					}
					adminLog('warn', 'Admin disconnecting all clients and servers');

					// Close all client connections — cleanupClient() fires via removeConnection()
					for (const [id, cws] of llmClients) {
						sendJSON(cws, { type: 'ERROR', message: 'Disconnected by admin' });
						cws.close(4002, 'Disconnected by admin');
					}

					// Close all server connections — cleanupServer() fires via removeConnection()
					for (const [id, sws] of llmServers) {
						sendJSON(sws, { type: 'ERROR', message: 'Disconnected by admin' });
						sws.close(4002, 'Disconnected by admin');
					}

					// pendingRequests is cleared by cleanupClient/cleanupServer calls above
					sendJSON(ws, { type: 'OK', message: 'All clients and servers disconnected' });
					break;
				}

				default:
					sendJSON(ws, { type: 'ERROR', message: `Unknown message type: ${msg.type}` });
			}
		});

		// ---- Heartbeat (protocol-level ping/pong) ----

		ws.on('pong', () => {
			ws.isAlive = true;
			if (ws._pongTimeout) {
				clearTimeout(ws._pongTimeout);
				ws._pongTimeout = null;
			}
		});

		ws.on('close', () => removeConnection(ws));
		ws.on('error', () => removeConnection(ws));

		// Initial ping
		ws.ping();
		ws._pongTimeout = setTimeout(() => {
			ws.terminate();
			removeConnection(ws);
		}, PONG_TIMEOUT_MS);
	});

	// ---- Periodic heartbeat ----

	const heartbeatInterval = setInterval(() => {
		for (const ws of allConnections) {
			if (ws.readyState !== WebSocket.OPEN) {
				allConnections.delete(ws);
				continue;
			}
			if (ws.isAlive === false) {
				ws.terminate();
				removeConnection(ws);
				continue;
			}
			ws.isAlive = false;
			ws.ping();
			ws._pongTimeout = setTimeout(() => {
				ws.terminate();
				removeConnection(ws);
			}, PONG_TIMEOUT_MS);
		}
	}, PING_INTERVAL_MS);

	// ---- Periodic admin status broadcast ----

	const adminStatusInterval = setInterval(() => {
		if (adminConnections.size > 0) {
			broadcastStatus();
		}
	}, ADMIN_UPDATE_INTERVAL_MS);

	wss.on('close', () => {
		clearInterval(heartbeatInterval);
		clearInterval(adminStatusInterval);
	});

	console.log('[WebSocket Relay] Server attached (shares HTTP port)');
	return wss;
}

module.exports = { attachWebSocketServer };
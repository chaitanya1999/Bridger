const WebSocket = require('ws');

/**
 * Shared WebSocket Relay Client
 *
 * Connects to the bridger relay server and handles the relay protocol.
 * Streaming-agnostic — emits 'token' events for each chunk regardless
 * of whether the source is streaming or not.
 *
 * Events:
 *   'registered'   → { role, clientId/serverId }
 *   'request'      → { requestId, clientId, model, messages, stream }
 *   'token'        → { requestId, delta }
 *   'complete'     → { requestId }
 *   'error'        → { requestId?, message }
 *   'list_models'  → { requestId }                 (only LLM_SERVER)
 *   'models_list'  → { requestId, models }         (only LLM_CLIENT)
 *   'cancel'       → { requestId }
 *   'disconnected' → { reason }
 *   'reconnecting' → { attempt, delay }
 *   'connected'    → {}
 */

const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 30_000;
const RECONNECT_JITTER = 0.2;

/**
 * Default log filter — log everything except TOKEN (too noisy during streaming).
 */
const DEFAULT_LOG_FILTER = {
	REGISTER: true,
	REGISTERED: true,
	REQUEST: true,
	TOKEN: true,
	COMPLETE: true,
	ERROR: true,
	CANCEL: true,
	LIST_MODELS: true,
	MODELS_LIST: true,
};

class RelayClient extends require('events').EventEmitter {
	/**
	 * @param {string} url  WebSocket URL of the relay server (e.g. ws://localhost:3000)
	 * @param {object} opts
	 * @param {string} opts.role          'LLM_CLIENT' or 'LLM_SERVER'
	 * @param {string} [opts.clientId]    Required for LLM_CLIENT
	 * @param {string} [opts.serverId]    Optional for LLM_SERVER (auto-generated if omitted)
	 * @param {string} [opts.secret]      Shared secret for LLM_SERVER auth
	 * @param {boolean} [opts.autoReconnect=true]
	 * @param {object} [opts.logFilter]   Map of message type → boolean (enabled/disabled).
	 *                                    Overrides DEFAULT_LOG_FILTER per type.
	 *                                    Set to null or { all: false } to silence all logs.
	 */
	constructor(url, opts = {}) {
		super();
		this.url = url;
		this.role = opts.role;
		this.clientId = opts.clientId;
		this.serverId = opts.serverId;
		this.secret = opts.secret;
		this.autoReconnect = opts.autoReconnect !== false;

		// Build log filter: start with default, overlay any user-provided overrides
		this._logFilter = { ...DEFAULT_LOG_FILTER };
		if (opts.logFilter) {
			if (opts.logFilter.all === false) {
				// Silence all
				for (const key of Object.keys(this._logFilter)) {
					this._logFilter[key] = false;
				}
			} else {
				for (const [key, val] of Object.entries(opts.logFilter)) {
					if (key in this._logFilter) {
						this._logFilter[key] = val;
					}
				}
			}
		}

		this.ws = null;
		this.registered = false;
		this._reconnectAttempt = 0;
		this._reconnectTimer = null;
		this._intentionalClose = false;
	}

	/** Format a message for logging — shows type, requestId, and relevant metadata. */
	_formatMsg(obj) {
		let s = obj.type;
		if (obj.requestId) s += ` ${obj.requestId}`;
		if (obj.model) s += ` (model=${obj.model})`;
		if (obj.type === 'TOKEN' && obj.delta) {
			const preview = (obj.delta.content || '').slice(0, 40);
			s += ` (${preview.length} chars: "${preview}...")`;
		}
		if (obj.type === 'MODELS_LIST' && Array.isArray(obj.models)) {
			s += ` (${obj.models.length} models)`;
		}
		if (obj.type === 'ERROR' && obj.message) {
			s += ` "${obj.message.slice(0, 60)}"`;
		}
		if (obj.type === 'REGISTER') {
			s += ` (${obj.role}${obj.clientId ? ', ' + obj.clientId : ''}${obj.serverId ? ', ' + obj.serverId : ''})`;
		}
		return s;
	}

	// ---- Public API ----

	/** Open the connection and register with the relay. */
	connect() {
		return new Promise((resolve, reject) => {
			if (this.ws) {
				reject(new Error('Already connected or connecting'));
				return;
			}

			this._intentionalClose = false;
			const ws = new WebSocket(this.url);
			this.ws = ws;

			ws.on('open', () => {
				this._reconnectAttempt = 0;
				this.emit('connected');

				// Send REGISTER
				const registerMsg = { type: 'REGISTER', role: this.role };
				if (this.role === 'LLM_CLIENT') {
					registerMsg.clientId = this.clientId;
				} else if (this.role === 'LLM_SERVER') {
					registerMsg.serverId = this.serverId;
					if (this.secret) registerMsg.secret = this.secret;
				}
				if (this._shouldLog('REGISTER')) {
					console.log(`[RelayClient:${this.role}] → ${this._formatMsg(registerMsg)}`);
				}
				ws.send(JSON.stringify(registerMsg));
			});

			ws.on('message', (raw) => {
				let msg;
				try {
					msg = JSON.parse(raw.toString());
				} catch {
					return;
				}

				if (this._shouldLog(msg.type)) {
					console.log(`[RelayClient:${this.role}] ← ${this._formatMsg(msg)}`);
				}

				switch (msg.type) {
					case 'REGISTERED':
						this.registered = true;
						if (this.role === 'LLM_CLIENT') {
							this.clientId = msg.clientId;
						} else if (this.role === 'LLM_SERVER') {
							this.serverId = msg.serverId;
						}
						this.emit('registered', msg);
						resolve(msg);
						break;

					case 'REQUEST':
						this.emit('request', msg);
						break;

					case 'TOKEN':
						this.emit('token', msg);
						break;

					case 'COMPLETE':
						this.emit('complete', msg);
						break;

					case 'ERROR':
						this.emit('error', msg);
						// If we haven't registered yet, reject the connect promise
						if (!this.registered) {
							reject(new Error(msg.message));
						}
						break;

					case 'LIST_MODELS':
						this.emit('list_models', msg);
						break;

					case 'MODELS_LIST':
						this.emit('models_list', msg);
						break;

					case 'CANCEL':
						this.emit('cancel', msg);
						break;

					default:
						// Unknown message type — ignore
						break;
				}
			});

			ws.on('close', (code, reason) => {
				this.registered = false;
				this.ws = null;
				this.emit('disconnected', { code, reason: reason?.toString() });
				this._scheduleReconnect();
			});

			ws.on('error', (err) => {
				// 'close' will fire after this, so reconnect is handled there
				this.emit('error', { message: err.message });
				if (!this.registered) {
					reject(err);
				}
			});

			// Timeout for registration
			setTimeout(() => {
				if (!this.registered) {
					ws.close();
					reject(new Error('REGISTER timeout'));
				}
			}, 10_000);
		});
	}

	/** Send a REQUEST message to the relay. */
	sendRequest(requestId, model, messages, stream = true) {
		this._send({
			type: 'REQUEST',
			requestId,
			model,
			messages,
			stream,
		});
	}

	/** Send a TOKEN message (only valid for LLM_SERVER). */
	sendToken(requestId, delta) {
		this._send({ type: 'TOKEN', requestId, delta });
	}

	/** Send a COMPLETE message (only valid for LLM_SERVER). */
	sendComplete(requestId) {
		this._send({ type: 'COMPLETE', requestId });
	}

	/** Send an ERROR message. */
	sendError(requestId, message) {
		this._send({ type: 'ERROR', requestId, message });
	}

	/** Send a LIST_MODELS request to the relay. */
	sendListModels(requestId) {
		this._send({ type: 'LIST_MODELS', requestId });
	}

	/** Send a MODELS_LIST response (only valid for LLM_SERVER). */
	sendModelsList(requestId, models) {
		this._send({ type: 'MODELS_LIST', requestId, models });
	}

	/** Send a CANCEL message. */
	sendCancel(requestId) {
		this._send({ type: 'CANCEL', requestId });
	}

	/** Close the connection intentionally (no auto-reconnect). */
	disconnect() {
		this._intentionalClose = true;
		if (this._reconnectTimer) {
			clearTimeout(this._reconnectTimer);
			this._reconnectTimer = null;
		}
		if (this.ws) {
			this.ws.close();
			this.ws = null;
		}
		this.registered = false;
	}

	// ---- Internal ----

	_shouldLog(type) {
		return this._logFilter[type] === true;
	}

	_send(obj) {
		if (this._shouldLog(obj.type)) {
			console.log(`[RelayClient:${this.role}] → ${this._formatMsg(obj)}`);
		}
		if (this.ws && this.ws.readyState === WebSocket.OPEN) {
			this.ws.send(JSON.stringify(obj));
		} else {
			this.emit('error', { message: 'Not connected' });
		}
	}

	_scheduleReconnect() {
		if (!this.autoReconnect || this._intentionalClose) return;

		const attempt = ++this._reconnectAttempt;
		const delay = Math.min(
			RECONNECT_BASE_MS * Math.pow(2, attempt - 1),
			RECONNECT_MAX_MS
		);
		// Add jitter
		const jitter = delay * RECONNECT_JITTER * (Math.random() * 2 - 1);
		const actualDelay = Math.round(delay + jitter);

		this.emit('reconnecting', { attempt, delay: actualDelay });

		this._reconnectTimer = setTimeout(() => {
			if (this._intentionalClose) return;
			this.ws = null;
			this.connect().catch(() => {
				// connect() will emit 'error' and schedule another reconnect
			});
		}, actualDelay);
	}
}

module.exports = RelayClient;
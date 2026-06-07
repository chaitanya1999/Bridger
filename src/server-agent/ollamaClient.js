const http = require('http');

/**
 * Ollama API Client
 *
 * Calls Ollama's /api/chat endpoint and returns an async iterable
 * of token chunks, regardless of whether streaming is enabled or not.
 *
 * Usage:
 *   const ollama = new OllamaClient('http://localhost:11434');
 *   for await (const chunk of ollama.chat('llama3', messages, true)) {
 *     console.log(chunk.content);
 *   }
 *
 * Supports cancellation via AbortSignal.
 */

class OllamaClient {
	/**
	 * @param {string} [baseUrl='http://localhost:11434']  Ollama server URL
	 */
	constructor(baseUrl = 'http://localhost:11434') {
		this.baseUrl = baseUrl.replace(/\/+$/, '');
	}

	/**
	 * Send a chat request to Ollama.
	 *
	 * @param {string} model     Model name (e.g. 'llama3', 'gemma4')
	 * @param {Array}  messages  Array of { role, content } objects
	 * @param {boolean} [stream=true]  Whether to stream tokens
	 * @param {AbortSignal} [signal]  Optional abort signal for cancellation
	 * @returns {AsyncGenerator<{ content: string, done: boolean }>}
	 */
	async *chat(model, messages, stream = true, signal) {
		const url = new URL('/api/chat', this.baseUrl);
		const body = JSON.stringify({ model, messages, stream });

		let response;
		try {
			response = await this._request(url, body, null, signal);
		} catch (err) {
			if (err.message === 'Aborted') return;
			throw err;
		}

		if (stream) {
			// NDJSON stream — process each chunk as it arrives using events
			const chunks = [];
			let streamEnded = false;
			let streamError = null;

			response.on('data', (chunk) => chunks.push(chunk));

			response.on('end', () => { streamEnded = true; });

			response.on('error', (err) => { streamError = err; });

			if (signal) {
				signal.addEventListener('abort', () => {
					response.destroy();
					streamEnded = true;
				}, { once: true });
			}

			// Wait for stream completion, processing chunks as they come
			const allData = await new Promise((resolve, reject) => {
				const checkDone = () => {
					if (streamError) return reject(streamError);
					if (streamEnded) return resolve(chunks);
					// Check again in a short while
					setTimeout(checkDone, 50);
				};
				// Also listen for immediate end
				response.on('end', () => {
					setTimeout(() => {
						if (streamError) reject(streamError);
						else resolve(chunks);
					}, 10);
				});
				setTimeout(checkDone, 5000); // Fallback timeout
			});

			// Process all chunks
			let buffer = '';
			for (const chunk of allData) {
				buffer += chunk.toString();
				const lines = buffer.split('\n');
				buffer = lines.pop() || '';

				for (const line of lines) {
					const trimmed = line.trim();
					if (!trimmed) continue;
					try {
						const parsed = JSON.parse(trimmed);
						yield {
							content: parsed.message?.content || '',
							done: parsed.done === true,
						};
					} catch {
						// Skip malformed lines
					}
				}
			}

			// Process remaining buffer
			if (buffer.trim()) {
				try {
					const parsed = JSON.parse(buffer.trim());
					yield {
						content: parsed.message?.content || '',
						done: parsed.done === true,
					};
				} catch {
					// Ignore
				}
			}
		} else {
			// Single JSON response
			const text = await this._readAll(response);
			try {
				const result = JSON.parse(text);
				yield {
					content: result.message?.content || '',
					done: true,
				};
			} catch {
				throw new Error(`Failed to parse Ollama response: ${text.slice(0, 200)}`);
			}
		}
	}

	/**
	 * Fetch the list of available models from Ollama.
	 * @returns {Promise<Array<{ name: string, modified_at: string }>>}
	 */
	async listModels() {
		const url = new URL('/api/tags', this.baseUrl);
		const response = await this._request(url, null, 'GET');
		const text = await this._readAll(response);
		const result = JSON.parse(text);
		return (result.models || []).map((m) => ({
			name: m.name,
			modified_at: m.modified_at,
		}));
	}

	// ---- Internal ----

	_request(url, body, method, signal) {
		return new Promise((resolve, reject) => {
			const options = {
				hostname: url.hostname,
				port: url.port,
				path: url.pathname,
				method: method || (body ? 'POST' : 'GET'),
				headers: { 'content-type': 'application/json' },
				timeout: 300_000,
			};

			if (body) {
				options.headers['content-length'] = Buffer.byteLength(body);
			}

			const req = http.request(options, (res) => {
				if (res.statusCode < 200 || res.statusCode >= 300) {
					let data = '';
					res.on('data', (chunk) => (data += chunk));
					res.on('end', () => reject(new Error(`Ollama returned ${res.statusCode}: ${data.slice(0, 200)}`)));
					return;
				}
				resolve(res);
			});

			req.on('error', reject);
			req.on('timeout', () => { req.destroy(); reject(new Error('Ollama request timed out')); });

			if (signal) {
				signal.addEventListener('abort', () => {
					req.destroy();
					reject(new Error('Aborted'));
				}, { once: true });
			}

			if (body) req.write(body);
			req.end();
		});
	}

	_readAll(response) {
		return new Promise((resolve, reject) => {
			let data = '';
			response.on('data', (chunk) => (data += chunk));
			response.on('end', () => resolve(data));
			response.on('error', reject);
		});
	}
}

module.exports = OllamaClient;
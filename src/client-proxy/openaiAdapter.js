/**
 * OpenAI Adapter
 *
 * Converts between OpenAI-compatible chat completion format and
 * the internal relay protocol.
 *
 * Handles both streaming (SSE) and non-streaming (JSON) responses.
 */

/**
 * Transform an incoming OpenAI-format request body into a relay REQUEST.
 *
 * @param {object} openaiReq  The OpenAI /v1/chat/completions request body
 * @returns {{ model: string, messages: Array, stream: boolean }}
 */
function toRelayRequest(openaiReq) {
	const { model, messages, stream = false } = openaiReq;

	if (!model) {
		throw new Error('model is required');
	}
	if (!messages || !Array.isArray(messages) || messages.length === 0) {
		throw new Error('messages array is required');
	}

	// Normalize content: OpenAI allows string or array of content parts
	function normalizeContent(content) {
		if (typeof content === 'string') return content;
		if (Array.isArray(content)) {
			// Extract text from content parts (e.g. [{"type":"text","text":"hello"}])
			return content
				.filter((p) => p.type === 'text')
				.map((p) => p.text || '')
				.join(' ');
		}
		return String(content || '');
	}

	return {
		model,
		messages: messages.map((m) => ({
			role: m.role || 'user',
			content: normalizeContent(m.content),
		})),
		stream,
	};
}

/**
 * Create an OpenAI-compatible SSE stream transformer.
 *
 * Returns an object with:
 *   - headers: HTTP headers for the SSE response
 *   - onToken(requestId, delta): call when a TOKEN is received
 *   - onComplete(requestId): call when COMPLETE is received
 *   - onError(requestId, message): call when ERROR is received
 *   - end(): call to finalize the stream
 *
 * @param {object} res  Express response object
 * @param {string} model  Model name (for response metadata)
 * @param {string} [id]  Optional response ID (auto-generated if omitted)
 * @returns {object}
 */
function createSSEResponse(res, model, id) {
	const responseId = id || `chatcmpl-${Date.now()}`;
	const created = Math.floor(Date.now() / 1000);
	let isEnded = false;

	const headers = {
		'Content-Type': 'text/event-stream',
		'Cache-Control': 'no-cache',
		'Connection': 'keep-alive',
		'X-Accel-Buffering': 'no',
	};

	function sendSSE(data) {
		if (isEnded) return;
		res.write(`data: ${JSON.stringify(data)}\n\n`);
	}

	return {
		headers,

		onToken(requestId, delta) {
			sendSSE({
				id: responseId,
				object: 'chat.completion.chunk',
				created,
				model,
				choices: [
					{
						index: 0,
						delta: {
							content: delta.content || '',
						},
						finish_reason: null,
					},
				],
			});
		},

		onComplete(requestId) {
			if (isEnded) return;
			isEnded = true;
			sendSSE({
				id: responseId,
				object: 'chat.completion.chunk',
				created,
				model,
				choices: [
					{
						index: 0,
						delta: {},
						finish_reason: 'stop',
					},
				],
			});
			res.write('data: [DONE]\n\n');
			res.end();
		},

		onError(requestId, message) {
			if (isEnded) return;
			isEnded = true;
			// Try to send an error SSE event, then end
			try {
				sendSSE({
					id: responseId,
					object: 'chat.completion.chunk',
					created,
					model,
					choices: [
						{
							index: 0,
							delta: {},
							finish_reason: 'error',
						},
					],
				});
				res.write('data: [DONE]\n\n');
			} catch {
				// Response may already be closed
			}
			res.end();
		},

		end() {
			if (!isEnded) {
				isEnded = true;
				res.end();
			}
		},
	};
}

/**
 * Create a non-streaming (single JSON) response.
 *
 * Returns an object with:
 *   - headers: HTTP headers
 *   - onToken(requestId, delta): accumulates tokens
 *   - onComplete(requestId): sends the final JSON response
 *   - onError(requestId, message): sends an error JSON response
 *   - end(): finalize if incomplete
 *
 * @param {object} res  Express response object
 * @param {string} model  Model name
 * @param {string} [id]  Optional response ID
 * @returns {object}
 */
function createJSONResponse(res, model, id) {
	const responseId = id || `chatcmpl-${Date.now()}`;
	const created = Math.floor(Date.now() / 1000);
	let content = '';
	let isEnded = false;

	const headers = {
		'Content-Type': 'application/json',
	};

	function buildFinalBody() {
		return {
			id: responseId,
			object: 'chat.completion',
			created,
			model,
			choices: [
				{
					index: 0,
					message: {
						role: 'assistant',
						content,
					},
					finish_reason: 'stop',
				},
			],
			usage: {
				prompt_tokens: -1,
				completion_tokens: -1,
				total_tokens: -1,
			},
		};
	}

	function sendFinal() {
		if (isEnded) return;
		isEnded = true;
		// Headers already sent by caller — just end with JSON body
		res.end(JSON.stringify(buildFinalBody()));
	}

	return {
		headers,

		onToken(requestId, delta) {
			content += delta.content || '';
		},

		onComplete(requestId) {
			sendFinal();
		},

		onError(requestId, message) {
			if (isEnded) return;
			isEnded = true;
			// Send 500 inline — if headers already sent this would fail,
			// so we send an error JSON and let the client handle it
			if (res.headersSent) {
				res.end(JSON.stringify({
					error: { message, type: 'server_error', param: null, code: null },
				}));
			} else {
				res.status(500).json({
					error: { message, type: 'server_error', param: null, code: null },
				});
			}
		},

		end() {
			if (!isEnded) {
				isEnded = true;
				res.end();
			}
		},
	};
}

module.exports = { toRelayRequest, createSSEResponse, createJSONResponse };
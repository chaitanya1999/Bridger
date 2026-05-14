const express = require('express');
const http = require('http');
const https = require('https');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: process.env.JSON_LIMIT || '10mb' }));
app.use(express.static(path.join(__dirname, '..', 'public')));

app.get('/health', (_req, res) => {
	res.json({ ok: true });
});

app.post('/bridge', (req, res) => {
	try {
		const { endpoint, method = 'GET', headers = {}, body } = req.body || {};

		if (!endpoint) {
			return res.status(400).json({ error: '`endpoint` is required' });
		}

		const target = new URL(endpoint);
		if (target.protocol !== 'http:' && target.protocol !== 'https:') {
			return res.status(400).json({ error: '`endpoint` must use http or https' });
		}

		const requestHeaders = cleanHeaders(headers);
		const requestBody = buildRequestBody(body, requestHeaders);
		const requestMethod = String(method).toUpperCase();

		if (requestBody && requestMethod !== 'GET' && requestMethod !== 'HEAD') {
			requestHeaders['content-length'] = Buffer.byteLength(requestBody);
		}

		const client = target.protocol === 'https:' ? https : http;
		const proxyRequest = client.request(
			target,
			{
				method: requestMethod,
				headers: requestHeaders
			},
			(proxyResponse) => {
				res.statusCode = proxyResponse.statusCode || 502;
				res.statusMessage = proxyResponse.statusMessage || res.statusMessage;

				for (const [name, value] of Object.entries(proxyResponse.headers)) {
					if (value !== undefined && !isHopByHopHeader(name)) {
						res.setHeader(name, value);
					}
				}

				proxyResponse.pipe(res);
			}
		);

		proxyRequest.on('error', (error) => {
			if (!res.headersSent) {
				res.status(502).json({ error: error.message });
			} else {
				res.destroy(error);
			}
		});

		if (requestBody && requestMethod !== 'GET' && requestMethod !== 'HEAD') {
			proxyRequest.write(requestBody);
		}

		proxyRequest.end();
	} catch (error) {
		res.status(400).json({ error: error.message });
	}
});

app.listen(PORT, () => {
	console.log(`Bridger listening on port ${PORT}`);
});

function buildRequestBody(body, headers) {
	if (body === undefined || body === null) {
		return null;
	}

	if (typeof body === 'string') {
		return Buffer.from(body);
	}

	if (!hasHeader(headers, 'content-type')) {
		headers['content-type'] = 'application/json';
	}

	return Buffer.from(JSON.stringify(body));
}

function cleanHeaders(headers) {
	if (!headers || typeof headers !== 'object' || Array.isArray(headers)) {
		return {};
	}

	const cleaned = {};

	for (const [name, value] of Object.entries(headers)) {
		if (isHopByHopHeader(name) || name.toLowerCase() === 'host' || value === undefined) {
			continue;
		}

		cleaned[name] = Array.isArray(value) ? value.map(String) : String(value);
	}

	return cleaned;
}

function hasHeader(headers, wantedHeader) {
	return Object.keys(headers || {}).some((name) => name.toLowerCase() === wantedHeader);
}

function isHopByHopHeader(name) {
	return [
		'connection',
		'keep-alive',
		'proxy-authenticate',
		'proxy-authorization',
		'te',
		'trailer',
		'transfer-encoding',
		'upgrade'
	].includes(name.toLowerCase());
}

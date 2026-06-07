#!/usr/bin/env node
/**
 * LLM-Client Proxy — Entry Point
 *
 * Usage:
 *   node client-proxy/index.js
 *
 * Environment variables:
 *   PORT            Local HTTP port (default: 3001)
 *   RELAY_URL       WebSocket URL of the relay server (default: ws://localhost:3000)
 *   CLIENT_ID       Client ID for relay registration (default: auto-generated)
 *   RELAY_SECRET    Optional shared secret for relay authentication
 *   API_KEY         Expected API key from clients (default: 'dummy')
 */

const { createClientProxy } = require('./app');

const PORT = parseInt(process.env.PORT, 10) || 3001;
const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:3000';
const CLIENT_ID = process.env.CLIENT_ID;
const API_KEY = process.env.API_KEY || 'dummy';

async function main() {
	const proxy = await createClientProxy(RELAY_URL, {
		port: PORT,
		apiKey: API_KEY,
		clientId: CLIENT_ID,
		autoReconnect: true,
	});
	const actualPort = await proxy.start();
	console.log(`[Client-Proxy] OpenAI-compatible endpoint: http://localhost:${actualPort}/v1`);
	console.log(`[Client-Proxy] Expected API key: ${API_KEY}`);

	// Graceful shutdown
	process.on('SIGINT', () => {
		console.log('\n[Client-Proxy] Shutting down...');
		proxy.stop();
		process.exit(0);
	});
	process.on('SIGTERM', () => {
		proxy.stop();
		process.exit(0);
	});
}

main().catch((err) => {
	console.error('[Client-Proxy] Fatal error:', err.message);
	process.exit(1);
});
#!/usr/bin/env node
/**
 * LLM-Server Agent — Entry Point
 *
 * Usage:
 *   node server-agent/index.js
 *
 * Environment variables:
 *   RELAY_URL       WebSocket URL of the relay server (default: ws://localhost:3000)
 *   SERVER_ID       Optional server ID (auto-generated if omitted)
 *   RELAY_SECRET    Optional shared secret for authentication
 *   OLLAMA_URL      Ollama server URL (default: http://localhost:11434)
 */

const { createServerAgent } = require('./app');

const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:3000';
const SERVER_ID = process.env.SERVER_ID;
const RELAY_SECRET = process.env.RELAY_SECRET;
const OLLAMA_URL = process.env.OLLAMA_URL || 'http://localhost:11434';

async function main() {
	console.log(`[Server-Agent] Starting — relay: ${RELAY_URL}, ollama: ${OLLAMA_URL}`);
	const agent = await createServerAgent(RELAY_URL, OLLAMA_URL, {
		serverId: SERVER_ID,
		secret: RELAY_SECRET,
		autoReconnect: true,
	});

	// Graceful shutdown
	process.on('SIGINT', () => {
		console.log('\n[Server-Agent] Shutting down...');
		agent.stop();
		process.exit(0);
	});
	process.on('SIGTERM', () => {
		agent.stop();
		process.exit(0);
	});
}

main().catch((err) => {
	console.error('[Server-Agent] Fatal error:', err.message);
	process.exit(1);
});
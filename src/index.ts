/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import { Client } from 'pg';

interface Env {
	POSTGRES_URL: string;
}

const queryCurrentTraffic = `SELECT yellow, red, dark_red, ts, x, y FROM traffic
	WHERE ts = (SELECT MAX(ts) FROM traffic)`;
const queryCongestedTraffic = `SELECT x, y, yellow, red, dark_red FROM traffic
	WHERE ts = (SELECT MAX(ts) FROM traffic)
	ORDER BY (yellow + red + dark_red) DESC
	LIMIT 10`;

const commonHeaders = {
	'Content-Type': 'application/json',
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, OPTIONS',
};

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);
		switch (url.pathname) {
			case '/current':
				return await handleCurrent(env);

			case '/congested':
				return await handleCongested(env);

			case '/health':
				return new Response('{"status": "ok"}', {
					status: 200,
					headers: commonHeaders,
				});

			default:
				return new Response('', { status: 404 });
		}
	},
} satisfies ExportedHandler<Env>;

async function handleCurrent(env: Env): Promise<Response> {
	const sql = new Client({ connectionString: env.POSTGRES_URL });
	await sql.connect();

	const result = await sql.query(queryCurrentTraffic);
	return new Response(JSON.stringify(result.rows), {
		headers: commonHeaders,
	});
}

async function handleCongested(env: Env): Promise<Response> {
	const sql = new Client({ connectionString: env.POSTGRES_URL });
	await sql.connect();

	const result = await sql.query(queryCongestedTraffic);
	return new Response(JSON.stringify(result.rows), {
		headers: commonHeaders,
	});
}

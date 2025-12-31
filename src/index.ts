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

const queryCurrentTraffic = `WITH grid_stats AS (
		-- Historical severity percentiles per grid
		SELECT
			x,
			y,
			percentile_cont(0.95)
				WITHIN GROUP (
					ORDER BY yellow * 1 + red * 2 + dark_red * 3
				) AS p95,
			percentile_cont(0.99)
				WITHIN GROUP (
					ORDER BY yellow * 1 + red * 2 + dark_red * 3
				) AS p99
		FROM traffic
		GROUP BY x, y
	),
	latest_per_grid AS (
		-- Latest record per grid in last 30 minutes
		SELECT DISTINCT ON (x, y)
			x,
			y,
			ts,
			yellow,
			red,
			dark_red,
			(yellow * 1 + red * 2 + dark_red * 3) AS latest_severity
		FROM traffic
		WHERE ts >= NOW() - INTERVAL '30 minutes'
		ORDER BY x, y, ts DESC
	)
	SELECT
		l.x,
		l.y,
		l.ts,
		l.yellow,
		l.red,
		l.dark_red,
		l.latest_severity,
		g.p95,
		g.p99
	FROM latest_per_grid l
	JOIN grid_stats g USING (x, y)
	ORDER BY l.latest_severity DESC;`;

const queryHistory = `SELECT x, y, ts, yellow, red, dark_red FROM traffic WHERE
	x = $1 AND y = $2 AND ts >= NOW() - CAST($3 AS INTERVAL) ORDER BY ts DESC`;

const queryHistoryHourly = `SELECT x, y, ts, yellow, red, dark_red FROM traffic WHERE
	x = $1 AND y = $2 AND ts >= NOW() - CAST($3 AS INTERVAL) AND EXTRACT(HOUR FROM ts) = $4 ORDER BY ts DESC`;

const querySustained = `WITH severity_threshold AS (
		SELECT
			percentile_cont(0.95)
			WITHIN GROUP (ORDER BY yellow*1 + red*2 + dark_red*3) AS p95
		FROM traffic
	),
	recent_data AS (
		SELECT
			x,
			y,
			ts,
			yellow,
			red,
			dark_red,
			(yellow*1 + red*2 + dark_red*3) AS severity
		FROM traffic
		WHERE ts >= NOW() - INTERVAL '2 hours'
	),
	marked AS (
		SELECT
			*,
			CASE
				WHEN severity >= (SELECT p95 FROM severity_threshold)
				THEN 1 ELSE 0
			END AS is_congested
		FROM recent_data
	),
	with_gaps AS (
		SELECT
			*,
			LEAD(ts) OVER (PARTITION BY x, y ORDER BY ts) AS next_ts
		FROM marked
	),
	segments AS (
		SELECT
			x,
			y,
			ts,
			CASE
				WHEN is_congested = 1
				AND next_ts IS NOT NULL
				AND next_ts - ts <= INTERVAL '20 minutes'
				THEN next_ts - ts
				ELSE INTERVAL '0'
			END AS duration_piece,
			CASE
				WHEN is_congested = 1
				AND (next_ts IS NULL OR next_ts - ts > INTERVAL '20 minutes')
				THEN 1 ELSE 0
			END AS break_point
		FROM with_gaps
	),
	runs AS (
		SELECT
			x,
			y,
			ts,
			duration_piece,
			SUM(break_point) OVER (
				PARTITION BY x, y
				ORDER BY ts
			) AS run_id
		FROM segments
		WHERE duration_piece > INTERVAL '0'
	),
	long_runs AS (
		SELECT
			x,
			y,
			run_id,
			SUM(duration_piece) AS total_duration
		FROM runs
		GROUP BY x, y, run_id
		HAVING SUM(duration_piece) >= INTERVAL '30 minutes'
	),
	latest_state AS (
		SELECT DISTINCT ON (x, y)
			x,
			y,
			ts AS latest_ts,
			yellow,
			red,
			dark_red,
			(yellow*1 + red*2 + dark_red*3) AS latest_severity
		FROM traffic
		ORDER BY x, y, ts DESC
	)
	SELECT
		l.x,
		l.y,
		s.latest_ts,
		s.yellow,
		s.red,
		s.dark_red,
		s.latest_severity,
		t.p95 AS threshold_p95
	FROM long_runs l
	JOIN latest_state s USING (x, y)
	CROSS JOIN severity_threshold t
	WHERE s.latest_severity >= t.p95
	ORDER BY s.latest_severity DESC;`;

const maxDurations: Record<string, string> = {
	'1h': '1h',
	'6h': '6h',
	'12h': '12h',
	'24h': '24h',
	'7d': '7d',
};

const commonHeaders = {
	'Content-Type': 'application/json',
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, OPTIONS',
};

export default {
	async fetch(request, env, _ctx): Promise<Response> {
		const url = new URL(request.url);
		switch (url.pathname) {
			case '/current':
				return await handleCurrent(env);

			case '/history':
				return await handleHistory(request, env);

			case '/history-hourly':
				return await handleHistoryHourly(request, env);

			case '/sustained':
				return await handleSustained(env);

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

async function handleHistory(request: Request, env: Env): Promise<Response> {
	const url = new URL(request.url);
	const duration = limitMaxDuration(url.searchParams.get('duration'));
	const x = url.searchParams.get('x') || '11';
	const y = url.searchParams.get('y') || '8';

	const sql = new Client({ connectionString: env.POSTGRES_URL });
	await sql.connect();

	const result = await sql.query(queryHistory, [x, y, duration]);
	return new Response(JSON.stringify(result.rows), {
		headers: commonHeaders,
	});
}

async function handleHistoryHourly(request: Request, env: Env): Promise<Response> {
	const url = new URL(request.url);
	const duration = limitMaxDuration(url.searchParams.get('duration'));
	const x = url.searchParams.get('x') || '11';
	const y = url.searchParams.get('y') || '8';
	const hour = url.searchParams.get('hour') || '15';

	const sql = new Client({ connectionString: env.POSTGRES_URL });
	await sql.connect();

	const result = await sql.query(queryHistoryHourly, [x, y, duration, hour]);
	return new Response(JSON.stringify(result.rows), {
		headers: commonHeaders,
	});
}

async function handleSustained(env: Env): Promise<Response> {
	const sql = new Client({ connectionString: env.POSTGRES_URL });
	await sql.connect();

	const result = await sql.query(querySustained);
	return new Response(JSON.stringify(result.rows), {
		headers: commonHeaders,
	});
}

function limitMaxDuration(duration: string | null): string {
	if (!duration) return '24h';
	return maxDurations[duration] || '24h';
}

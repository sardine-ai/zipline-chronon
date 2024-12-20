import { env } from '$env/dynamic/private';
import type { RequestHandler } from './$types';

const API_BASE_URL = env.API_BASE_URL ?? 'http://localhost:9000';

/**
 * Proxy calls from frontend server (ex. `http://app.zipline.ai/api`) to Scala backend (ex. `http://app:9000/api`), resolving:
 *  - CORS issues
 *  - Consistent URL handling whether issuing requests from browser (ex. `+page.svelte`) or frontend server (ex `+page.server.ts`)
 */
export const GET: RequestHandler = ({ params, url, request }) => {
	return fetch(`${API_BASE_URL}/api/${params.path + url.search}`, request);
};

export const POST: RequestHandler = ({ params, url, request }) => {
	return fetch(`${API_BASE_URL}/api/${params.path + url.search}`, request);
};

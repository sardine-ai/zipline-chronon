import { error } from '@sveltejs/kit';
import { browser } from '$app/environment';

const apiBaseUrl = !browser
	? process?.env?.API_BASE_URL || 'http://localhost:9000'
	: 'http://localhost:9000';

const base = `${apiBaseUrl}/api/v1`;

async function send({ method, path }: { method: string; path: string }) {
	const opts = { method, headers: {} };

	const res = await fetch(`${base}/${path}`, opts);
	if (res.ok) {
		const text = await res.text();
		return text ? JSON.parse(text) : {};
	}

	error(res.status);
}

export function get(path: string) {
	return send({ method: 'GET', path });
}

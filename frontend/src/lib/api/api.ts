import { error } from '@sveltejs/kit';
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL;

const base = `${API_BASE_URL}/api/v1`;

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

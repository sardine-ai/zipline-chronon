import type {
	FeatureResponse,
	JoinTimeSeriesResponse,
	ModelsResponse,
	TimeSeriesResponse
} from '$lib/types/Model/Model';
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

// todo: eventually move this to a model-specific file/decide on a good project structure for organizing api calls
export async function getModels(): Promise<ModelsResponse> {
	return get('models');
}

export async function getModelTimeseries(
	name: string,
	startTs: number,
	endTs: number,
	offset: string = '10h',
	algorithm: string = 'psi'
): Promise<TimeSeriesResponse> {
	const params = new URLSearchParams({
		startTs: startTs.toString(),
		endTs: endTs.toString(),
		offset,
		algorithm
	});
	return get(`model/${name}/timeseries?${params.toString()}`);
}

export async function search(term: string, limit: number = 20): Promise<ModelsResponse> {
	const params = new URLSearchParams({
		term,
		limit: limit.toString()
	});
	return get(`search?${params.toString()}`);
}

export async function getJoinTimeseries(
	joinId: string,
	startTs: number,
	endTs: number,
	metricType: string = 'drift',
	metrics: string = 'null',
	offset: string = '10h',
	algorithm: string = 'psi'
): Promise<JoinTimeSeriesResponse> {
	const params = new URLSearchParams({
		startTs: startTs.toString(),
		endTs: endTs.toString(),
		metricType,
		metrics,
		offset,
		algorithm
	});

	return get(`join/${joinId}/timeseries?${params.toString()}`);
}

export async function getFeatureTimeseries(
	featureName: string,
	startTs: number,
	endTs: number,
	metricType: string = 'drift',
	metrics: string = 'null',
	offset: string = '10h',
	algorithm: string = 'psi',
	granularity: string = 'percentile'
): Promise<FeatureResponse> {
	const params = new URLSearchParams({
		startTs: startTs.toString(),
		endTs: endTs.toString(),
		metricType,
		metrics,
		offset,
		algorithm,
		granularity
	});
	return get(`feature/${featureName}/timeseries?${params.toString()}`);
}

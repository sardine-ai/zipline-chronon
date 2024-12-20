import { error } from '@sveltejs/kit';
import type {
	FeatureResponse,
	JoinsResponse,
	JoinTimeSeriesResponse,
	ModelsResponse
} from '$lib/types/Model/Model';

export type ApiOptions = {
	base?: string;
	fetch?: typeof fetch;
	accessToken?: string;
};

export type ApiRequestOptions = {
	method?: 'GET' | 'POST' | 'PUT' | 'DELETE';
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	data?: Record<string, any>;
	headers?: Record<string, string>;
};

export class Api {
	#base: string;
	#fetch: typeof fetch;
	#accessToken: string | undefined;

	constructor(opts: ApiOptions = {}) {
		this.#base = opts.base ?? '/api/v1';
		this.#fetch = opts.fetch ?? fetch; // default to global fetch (browser and node)
		this.#accessToken = opts.accessToken;
	}

	// TODO: eventually move this to a model-specific file/decide on a good project structure for organizing api calls
	async getModels() {
		return this.#send<ModelsResponse>('models');
	}

	async getJoins(offset: number = 0, limit: number = 10) {
		const params = new URLSearchParams({
			offset: offset.toString(),
			limit: limit.toString()
		});
		return this.#send<JoinsResponse>(`joins?${params.toString()}`);
	}

	async search(term: string, limit: number = 20) {
		const params = new URLSearchParams({
			term,
			limit: limit.toString()
		});
		return this.#send<ModelsResponse>(`search?${params.toString()}`);
	}

	async getJoinTimeseries({
		joinId,
		startTs,
		endTs,
		metricType = 'drift',
		metrics = 'null',
		offset = '10h',
		algorithm = 'psi'
	}: {
		joinId: string;
		startTs: number;
		endTs: number;
		metricType?: string;
		metrics?: string;
		offset?: string;
		algorithm?: string;
	}) {
		const params = new URLSearchParams({
			startTs: startTs.toString(),
			endTs: endTs.toString(),
			metricType,
			metrics,
			offset,
			algorithm
		});

		return this.#send<JoinTimeSeriesResponse>(`join/${joinId}/timeseries?${params.toString()}`);
	}

	async getFeatureTimeseries({
		joinId,
		featureName,
		startTs,
		endTs,
		metricType = 'drift',
		metrics = 'null',
		offset = '10h',
		algorithm = 'psi',
		granularity = 'aggregates'
	}: {
		joinId: string;
		featureName: string;
		startTs: number;
		endTs: number;
		metricType?: string;
		metrics?: string;
		offset?: string;
		algorithm?: string;
		granularity?: string;
	}) {
		const params = new URLSearchParams({
			startTs: startTs.toString(),
			endTs: endTs.toString(),
			metricType,
			metrics,
			offset,
			algorithm,
			granularity
		});
		return this.#send<FeatureResponse>(
			`join/${joinId}/feature/${featureName}/timeseries?${params.toString()}`
		);
	}

	async #send<Data = unknown>(resource: string, options?: ApiRequestOptions) {
		let url = `${this.#base}/${resource}`;

		const method = options?.method ?? 'GET';

		if (method === 'GET' && options?.data) {
			url += `?${new URLSearchParams(options.data)}`;
		}

		return this.#fetch(url, {
			method: options?.method ?? 'GET',
			headers: {
				'Content-Type': 'application/json',
				...(this.#accessToken && { Authorization: `Bearer ${this.#accessToken}` })
			},
			...(method === 'POST' &&
				options?.data && {
					body: JSON.stringify(options.data)
				})
		}).then(async (response) => {
			if (response.ok) {
				const text = await response.text();
				try {
					if (text) {
						return JSON.parse(text) as Data;
					} else {
						// TODO: Should we return `null` here and require users to handle
						return {} as Data;
					}
				} catch (e) {
					console.error(`Unable to parse: "${text}" for url: ${url}`);
					throw e;
				}
			} else {
				const text = (await response.text?.()) ?? '';
				console.error(`Failed request: "${text}" for url: ${url}`);
				error(response.status);
			}
		});
	}
}

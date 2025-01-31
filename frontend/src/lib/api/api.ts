import { error } from '@sveltejs/kit';
import type { FeatureResponse, JoinTimeSeriesResponse } from '$lib/types/Model/Model';
import type {
	Join,
	GroupBy,
	Model,
	StagingQuery,
	IJoinDriftRequestArgs,
	IJoinDriftResponseArgs,
	ITileSummarySeries,
	IJoinSummaryRequestArgs
} from '$lib/types/codegen';
import { ConfType, DriftMetric } from '$lib/types/codegen';
import type { ConfListResponse } from '$lib/types/codegen/ConfListResponse';

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

	async getConf(name: string, type: ConfType) {
		const params = new URLSearchParams({
			confName: name,
			confType: ConfType[type]
		});
		return this.#send<Join | GroupBy | Model>(`conf?${params.toString()}`);
	}

	async getJoin(name: string): Promise<Join> {
		return this.getConf(name, ConfType.JOIN) as Promise<Join>;
	}

	async getGroupBy(name: string): Promise<GroupBy> {
		return this.getConf(name, ConfType.GROUP_BY) as Promise<GroupBy>;
	}

	async getModel(name: string): Promise<Model> {
		return this.getConf(name, ConfType.MODEL) as Promise<Model>;
	}

	async getStagingQuery(name: string): Promise<StagingQuery> {
		return this.getConf(name, ConfType.STAGING_QUERY) as Promise<StagingQuery>;
	}

	async search(term: string) {
		const params = new URLSearchParams({
			confName: term
		});
		return this.#send<ConfListResponse>(`search?${params.toString()}`);
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

	async getConfList(type: ConfType): Promise<ConfListResponse> {
		const params = new URLSearchParams({
			confType: ConfType[type]
		});
		return this.#send<ConfListResponse>(`conf/list?${params.toString()}`);
	}

	async getJoinList(): Promise<ConfListResponse> {
		return this.getConfList(ConfType.JOIN);
	}

	async getGroupByList(): Promise<ConfListResponse> {
		return this.getConfList(ConfType.GROUP_BY);
	}

	async getModelList(): Promise<ConfListResponse> {
		return this.getConfList(ConfType.MODEL);
	}

	async getStagingQueryList(): Promise<ConfListResponse> {
		return this.getConfList(ConfType.STAGING_QUERY);
	}

	async getJoinDrift({
		name,
		startTs,
		endTs,
		offset = '10h',
		algorithm = DriftMetric.PSI
	}: IJoinDriftRequestArgs) {
		const params = new URLSearchParams({
			startTs: startTs.toString(),
			endTs: endTs.toString(),
			offset,
			algorithm: DriftMetric[algorithm]
		});
		return this.#send<IJoinDriftResponseArgs>(`join/${name}/drift?${params.toString()}`);
	}

	async getColumnDrift({
		name,
		columnName,
		startTs,
		endTs,
		offset = '10h',
		algorithm = DriftMetric.PSI
	}: IJoinDriftRequestArgs) {
		const params = new URLSearchParams({
			startTs: startTs.toString(),
			endTs: endTs.toString(),
			offset,
			algorithm: DriftMetric[algorithm]
		});
		return this.#send<IJoinDriftResponseArgs>(
			`join/${name}/column/${columnName}/drift?${params.toString()}`
		);
	}

	async getColumnSummary({ name, columnName, startTs, endTs }: IJoinSummaryRequestArgs) {
		const params = new URLSearchParams({
			startTs: startTs.toString(),
			endTs: endTs.toString()
		});
		return this.#send<ITileSummarySeries>(
			`join/${name}/column/${columnName}/summary?${params.toString()}`
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

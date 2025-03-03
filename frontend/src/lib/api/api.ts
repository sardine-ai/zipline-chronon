import { error } from '@sveltejs/kit';
import {
	type IConfListResponseArgs,
	type IJoinArgs,
	type IGroupByArgs,
	type IModelArgs,
	type IStagingQueryArgs,
	type IJoinDriftRequestArgs,
	type ITileSummarySeriesArgs,
	type IJoinSummaryRequestArgs,
	type IJoinDriftResponseArgs,
	type ILogicalNodeArgs,
	type ILineageRequestArgs,
	type ILineageResponseArgs,
	type IJobTrackerResponseArgs,
	type INodeKeyArgs,
	ConfType,
	DriftMetric,
	Status
} from '$lib/types/codegen';
import { confToLineage } from './utils';

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
		return this.#send<ILogicalNodeArgs>(`conf?${params.toString()}`);
	}

	async getJoin(name: string) {
		return this.getConf(name, ConfType.JOIN).then((d) => d.join) as Promise<IJoinArgs>;
	}

	async getGroupBy(name: string) {
		return this.getConf(name, ConfType.GROUP_BY).then((d) => d.groupBy) as Promise<IGroupByArgs>;
	}

	async getModel(name: string) {
		return this.getConf(name, ConfType.MODEL).then((d) => d.model) as Promise<IModelArgs>;
	}

	async getStagingQuery(name: string) {
		return this.getConf(name, ConfType.STAGING_QUERY).then(
			(d) => d.stagingQuery
		) as Promise<IStagingQueryArgs>;
	}

	async search(term: string) {
		const params = new URLSearchParams({
			confName: term
		});
		return this.#send<IConfListResponseArgs>(`search?${params.toString()}`);
	}

	async getConfList(type: ConfType): Promise<IConfListResponseArgs> {
		const params = new URLSearchParams({
			confType: ConfType[type]
		});
		return this.#send<IConfListResponseArgs>(`conf/list?${params.toString()}`);
	}

	async getJoinList(): Promise<IConfListResponseArgs> {
		return this.getConfList(ConfType.JOIN);
	}

	async getGroupByList(): Promise<IConfListResponseArgs> {
		return this.getConfList(ConfType.GROUP_BY);
	}

	async getModelList(): Promise<IConfListResponseArgs> {
		return this.getConfList(ConfType.MODEL);
	}

	async getStagingQueryList(): Promise<IConfListResponseArgs> {
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

	async getColumnSummary({
		name,
		columnName,
		startTs,
		endTs,
		percentiles = 'p5,p50,p95'
	}: IJoinSummaryRequestArgs & { percentiles?: string }) {
		const params = new URLSearchParams({
			startTs: startTs.toString(),
			endTs: endTs.toString(),
			percentiles
		});
		return this.#send<ITileSummarySeriesArgs>(
			`join/${name}/column/${columnName}/summary?${params.toString()}`
		);
	}

	async getJoinLineage({
		name
		// type,
		// branch,
		// direction
	}: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// const params = new URLSearchParams(
		// 	omitNil({
		// 		type,
		// 		branch,
		// 		direction: direction ? Direction[direction] : undefined
		// 	})
		// );
		// return this.#send<ILineageResponseArgs>(`join/${name}/lineage?${params.toString()}`);

		// TODO: Remove this once we have the API endpoint
		return this.getJoin(name!).then((join) => {
			return confToLineage(join);
		});
	}

	async getGroupByLineage({
		name
		// type,
		// branch,
		// direction
	}: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// TODO: Remove this once we have the API endpoint
		return this.getGroupBy(name!).then((groupBy) => {
			return confToLineage(groupBy);
		});
	}

	async getModelLineage({
		name
		// type,
		// branch,
		// direction
	}: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// TODO: Remove this once we have the API endpoint
		return this.getModel(name!).then((model) => {
			return confToLineage(model);
		});
	}

	async getStagingQueryLineage({ name }: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// TODO: Remove this once we have the API endpoint
		return this.getStagingQuery(name!).then((stagingQuery) => {
			return confToLineage(stagingQuery);
		});
	}

	async getJobTrackerData(
		node: INodeKeyArgs,
		daysToGenerate: number = 60
	): Promise<IJobTrackerResponseArgs> {
		const startDate = new Date('2024-01-01');
		const endDateLimit = new Date(startDate);
		endDateLimit.setDate(startDate.getDate() + daysToGenerate - 1);
		const tasks: IJobTrackerResponseArgs['tasks'] = [];

		let currentDate = new Date(startDate);
		while (currentDate <= endDateLimit) {
			const taskDuration = 2 + Math.floor(Math.random() * 3);
			const endDate = new Date(currentDate);
			endDate.setDate(endDate.getDate() + taskDuration);

			// If this task would go beyond our limit, adjust it to end at the limit
			if (endDate > endDateLimit) {
				endDate.setTime(endDateLimit.getTime());
			}

			const statusValues = Object.values(Status).filter((v): v is number => typeof v === 'number');

			tasks.push({
				status: statusValues[Math.floor(Math.random() * statusValues.length)],
				submittedTs: currentDate.getTime(),
				dateRange: {
					startDate: currentDate.toISOString().split('T')[0],
					endDate: endDate.toISOString().split('T')[0]
				}
			});

			// Add overlapping task with 30% chance, but only if we're not at the end
			if (Math.random() < 0.3 && endDate < endDateLimit) {
				const overlapStart = new Date(currentDate);
				overlapStart.setDate(overlapStart.getDate() + 1);
				const overlapEnd = new Date(endDate);
				overlapEnd.setDate(overlapEnd.getDate() + 1);

				// Ensure overlap end date doesn't exceed limit
				if (overlapEnd > endDateLimit) {
					overlapEnd.setTime(endDateLimit.getTime());
				}

				tasks.push({
					status: statusValues[Math.floor(Math.random() * statusValues.length)],
					submittedTs: overlapStart.getTime(),
					dateRange: {
						startDate: overlapStart.toISOString().split('T')[0],
						endDate: overlapEnd.toISOString().split('T')[0]
					}
				});
			}

			currentDate = new Date(endDate);
			currentDate.setDate(currentDate.getDate() + 1);
		}

		return {
			tasks,
			mainNode: node
		};
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

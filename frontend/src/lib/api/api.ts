import { error } from '@sveltejs/kit';
import { parseISO } from 'date-fns';

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
	type IJobTrackerRequestArgs
} from '$lib/types/codegen';
import { confToLineage } from './utils';
import { generateLineageTaskInfoData } from '../util/test-data/task-info';

import lineageTasks from '$lib/util/test-data/generated/task-info-lineage.json';
import { DEFAULT_OFFSET } from '../params/offset';

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
		offset = DEFAULT_OFFSET + 'd',
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
		offset = DEFAULT_OFFSET + 'd',
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
		const join = await this.getJoin(name!);

		// TODO: Replacing source temporarily for demo purposes
		if (join.metaData?.name === 'risk.user_transactions.txn_join') {
			// @ts-expect-error Ignore
			join.left.events.table = 'data.txn_events';
		}

		return confToLineage(join);
	}

	async getGroupByLineage({
		name
		// type,
		// branch,
		// direction
	}: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// TODO: Remove this once we have the API endpoint
		const groupBy = await this.getGroupBy(name!);
		return confToLineage(groupBy);
	}

	async getModelLineage({
		name
		// type,
		// branch,
		// direction
	}: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// TODO: Remove this once we have the API endpoint
		const model = await this.getModel(name!);
		return confToLineage(model);
	}

	async getStagingQueryLineage({ name }: ILineageRequestArgs): Promise<ILineageResponseArgs> {
		// TODO: Remove this once we have the API endpoint
		const stagingQuery = await this.getStagingQuery(name!);
		return confToLineage(stagingQuery);
	}

	async getJobTrackerData({
		name,
		dateRange
	}: IJobTrackerRequestArgs): Promise<IJobTrackerResponseArgs> {
		// const startDate = new Date(dateRange?.startDate ?? '2024-01-01');
		// const endDate = new Date(dateRange?.endDate ?? '2024-02-28');

		// TODO: Remove generated task data once we have the API endpoint
		const node: INodeKeyArgs = {
			name: name
			// TODO: way to determine this just based on the name?
			// logicalType: LogicalType.JOIN,
			// physicalType: PhysicalType.JOIN
		};
		// const tasks: ITaskInfoArgs[] = generateTaskInfoData({
		// 	startDate,
		// 	endDate
		// });

		// @ts-expect-error Ignore
		const tasks = lineageTasks[name ?? 'Unknown'] ?? [];

		return {
			mainNode: node,
			tasks
		};
	}

	// TODO: Temporary until we have orchestrator with API endpoint
	async getLineageJobTrackerData({
		name,
		dateRange
	}: IJobTrackerRequestArgs): Promise<ReturnType<typeof generateLineageTaskInfoData>> {
		const startDate = parseISO(dateRange?.startDate ?? '2024-01-01');
		const endDate = parseISO(dateRange?.endDate ?? '2024-02-28');

		// Get lineage based on node name
		let lineage: ILineageResponseArgs | null = null;
		try {
			if (name?.includes('join')) {
				lineage = await this.getJoinLineage({ name });
			} else if (name?.includes('group')) {
				lineage = await this.getGroupByLineage({ name });
			}
		} catch (e) {
			// Unable to get lineage unable (common for group by)
			// console.error(`Unable to get lineage for ${name}: ${e}`);
		}

		const lineageTasks = generateLineageTaskInfoData({
			node: lineage?.mainNode ?? { name },
			lineage,
			startDate,
			endDate
		});

		return lineageTasks;
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

import { Api } from '$lib/api/api';
import {
	DriftMetric,
	type IJoin,
	type IJoinDriftResponse,
	type ILineageResponse,
	type IModel,
	type IJobTrackerResponseArgs,
	type INodeKeyArgs
} from '$lib/types/codegen';

const FALLBACK_START_TS = 1672531200000; // 2023-01-01
const FALLBACK_END_TS = 1677628800000; // 2023-03-01

export type JoinData = {
	join: IJoin;
	lineage: ILineageResponse;
	model?: IModel;
	joinDrift?: IJoinDriftResponse;
	driftMetric: DriftMetric;
	dateRange: {
		startTimestamp: number;
		endTimestamp: number;
		dateRangeValue: string;
		isUsingFallback: boolean;
	};
	jobTrackerDataMap: Map<string, IJobTrackerResponseArgs>;
};

export async function getJoinData(
	api: Api,
	joinName: string,
	requestedDateRange: { startTimestamp: number; endTimestamp: number; dateRangeValue: string },
	driftMetric: DriftMetric
): Promise<JoinData> {
	const [join, lineage, model] = await Promise.all([
		api.getJoin(joinName),
		api.getJoinLineage({ name: joinName }),
		api
			.getModelList()
			.then((models) =>
				models.models?.find((m) => m.source?.joinSource?.join?.metaData?.name === joinName)
			)
	]);

	// Get job tracker data for all nodes in lineage
	const nodes = new Set<INodeKeyArgs>();

	// Add main nodes
	lineage.nodeGraph?.connections?.forEach((_, node) => {
		nodes.add(node);
	});

	// Add parent nodes
	lineage.nodeGraph?.connections?.forEach((connection) => {
		connection.parents?.forEach((parent) => {
			nodes.add(parent);
		});
	});

	// todo think about where this is best called - might be worth lazily lower levels
	const jobTrackerDataMap = new Map<string, IJobTrackerResponseArgs>();
	await Promise.all(
		Array.from(nodes).map(async (node) => {
			const data = await api.getJobTrackerData(node);
			jobTrackerDataMap.set(node.name ?? '', data);
		})
	);

	let dateRange = {
		...requestedDateRange,
		isUsingFallback: false
	};

	let joinDrift: IJoinDriftResponse | undefined = undefined;
	try {
		// Try with requested date range first
		joinDrift = await api.getJoinDrift({
			name: joinName,
			startTs: dateRange.startTimestamp,
			endTs: dateRange.endTimestamp,
			algorithm: driftMetric
		});

		// If empty data is results, use fallback date range
		const useFallback = joinDrift.driftSeries.every((ds) => Object.keys(ds).length <= 1); // Only `key` returned on results
		if (useFallback) {
			dateRange = {
				...dateRange,
				startTimestamp: FALLBACK_START_TS,
				endTimestamp: FALLBACK_END_TS,
				isUsingFallback: true
			};

			joinDrift = await api.getJoinDrift({
				name: joinName,
				startTs: dateRange.startTimestamp,
				endTs: dateRange.endTimestamp,
				algorithm: driftMetric
			});
		}
	} catch {
		// Drift series data not available for join
	}

	return {
		join,
		lineage,
		model,
		joinDrift,
		driftMetric,
		dateRange,
		jobTrackerDataMap
	};
}

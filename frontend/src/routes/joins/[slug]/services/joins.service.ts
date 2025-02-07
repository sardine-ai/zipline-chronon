import { Api } from '$lib/api/api';
import { DriftMetric, type IJoinDriftResponse, type IModel } from '$lib/types/codegen';

const FALLBACK_START_TS = 1672531200000; // 2023-01-01
const FALLBACK_END_TS = 1677628800000; // 2023-03-01

export type JoinData = {
	joinDrift: IJoinDriftResponse;
	model?: IModel;
	driftMetric: DriftMetric;
	dateRange: {
		startTimestamp: number;
		endTimestamp: number;
		dateRangeValue: string;
		isUsingFallback: boolean;
	};
};
export async function getJoinData(
	api: Api,
	joinName: string,
	requestedDateRange: { startTimestamp: number; endTimestamp: number; dateRangeValue: string },
	driftMetric: DriftMetric
): Promise<JoinData> {
	const model = await api
		.getModelList()
		.then((models) =>
			models.models?.find((m) => m.source?.joinSource?.join?.metaData?.name === joinName)
		);

	let dateRange = {
		...requestedDateRange,
		isUsingFallback: false
	};

	// Try with requested date range first
	let joinDrift = await api.getJoinDrift({
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

	return {
		model,
		joinDrift,
		driftMetric,
		dateRange
	};
}

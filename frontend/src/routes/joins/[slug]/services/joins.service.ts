import { Api } from '$lib/api/api';
import type { JoinTimeSeriesResponse } from '$lib/types/Model/Model';
import type { MetricType } from '$lib/types/MetricType/MetricType';
import { sortDrift, type SortDirection } from '$lib/util/sort';
import type { IModel } from '$lib/types/codegen';

const FALLBACK_START_TS = 1672531200000; // 2023-01-01
const FALLBACK_END_TS = 1677628800000; // 2023-03-01

export type JoinData = {
	joinTimeseries: JoinTimeSeriesResponse;
	model?: IModel;
	metricType: MetricType;
	dateRange: {
		startTimestamp: number;
		endTimestamp: number;
		dateRangeValue: string;
		isUsingFallback: boolean;
	};
};

async function fetchInitialData(
	api: Api,
	joinName: string,
	startTs: number,
	endTs: number,
	metricType: MetricType,
	sortDirection: SortDirection
) {
	const [joinTimeseries, models] = await Promise.all([
		api.getJoinTimeseries({
			joinId: joinName,
			startTs,
			endTs,
			metricType: 'drift',
			metrics: 'value',
			offset: undefined,
			algorithm: metricType
		}),
		api.getModelList()
	]);

	const sortedJoinTimeseries = sortDrift(joinTimeseries, sortDirection);
	const modelToReturn = models.models?.find(
		(m) => m.source?.joinSource?.join?.metaData?.name === joinName
	);

	return {
		joinTimeseries: sortedJoinTimeseries,
		model: modelToReturn
	};
}

export async function getJoinData(
	api: Api,
	joinName: string,
	requestedDateRange: { startTimestamp: number; endTimestamp: number; dateRangeValue: string },
	metricType: MetricType,
	sortDirection: SortDirection
): Promise<JoinData> {
	// Try with requested date range first
	try {
		const { joinTimeseries, model } = await fetchInitialData(
			api,
			joinName,
			requestedDateRange.startTimestamp,
			requestedDateRange.endTimestamp,
			metricType,
			sortDirection
		);

		return {
			joinTimeseries,
			model,
			metricType,
			dateRange: {
				...requestedDateRange,
				isUsingFallback: false
			}
		};
	} catch (error) {
		console.error('Error fetching data:', error);
		// If the requested range fails, fall back to the known working range
		const { joinTimeseries, model } = await fetchInitialData(
			api,
			joinName,
			FALLBACK_START_TS,
			FALLBACK_END_TS,
			metricType,
			sortDirection
		);

		return {
			joinTimeseries,
			model,
			metricType,
			dateRange: {
				startTimestamp: FALLBACK_START_TS,
				endTimestamp: FALLBACK_END_TS,
				dateRangeValue: requestedDateRange.dateRangeValue,
				isUsingFallback: true
			}
		};
	}
}

import type { PageServerLoad } from './$types';
import * as api from '$lib/api/api';
import type { JoinTimeSeriesResponse, Model } from '$lib/types/Model/Model';
import { parseDateRangeParams } from '$lib/util/date-ranges';
import { getMetricTypeFromParams, type MetricType } from '$lib/types/MetricType/MetricType';
import { getSortDirection, sortDrift, type SortDirection } from '$lib/util/sort';

const FALLBACK_START_TS = 1672531200000; // 2023-01-01
const FALLBACK_END_TS = 1677628800000; // 2023-03-01

export const load: PageServerLoad = async ({
	params,
	url
}): Promise<{
	joinTimeseries: JoinTimeSeriesResponse;
	model?: Model;
	metricType: MetricType;
	dateRange: {
		startTimestamp: number;
		endTimestamp: number;
		dateRangeValue: string;
		isUsingFallback: boolean;
	};
}> => {
	const requestedDateRange = parseDateRangeParams(url.searchParams);
	const joinName = params.slug;
	const metricType = getMetricTypeFromParams(url.searchParams);
	const sortDirection = getSortDirection(url.searchParams, 'drift');

	// Try with requested date range first
	try {
		const { joinTimeseries, model } = await fetchInitialData(
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
};

async function fetchInitialData(
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
		api.getModels()
	]);

	const sortedJoinTimeseries = sortDrift(joinTimeseries, sortDirection);
	const modelToReturn = models.items.find((m) => m.join.name === joinName);

	return {
		joinTimeseries: sortedJoinTimeseries,
		model: modelToReturn
	};
}

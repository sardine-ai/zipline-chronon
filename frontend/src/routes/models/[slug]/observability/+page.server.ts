import type { PageServerLoad } from './$types';
import * as api from '$lib/api/api';
import type { TimeSeriesResponse, JoinTimeSeriesResponse } from '$lib/types/Model/Model';
import { DATE_RANGE_PARAM, DATE_RANGES, type DateRangeOption } from '$lib/constants/date-ranges';
import { getDateRange } from '$lib/util/date-ranges';
import { LAST_7_DAYS } from '$lib/constants/date-ranges';

export const load: PageServerLoad = async ({
	params,
	url
}): Promise<{
	timeseries: TimeSeriesResponse;
	joinTimeseries: JoinTimeSeriesResponse;
	dateRange: DateRangeOption;
}> => {
	const dateRangeValue = url.searchParams.get(DATE_RANGE_PARAM) || LAST_7_DAYS;
	const [startTimestamp, endTimestamp] = getDateRange(dateRangeValue);

	const dateRange =
		DATE_RANGES.find((range) => range.value === dateRangeValue) ||
		DATE_RANGES.find((range) => range.value === LAST_7_DAYS) ||
		DATE_RANGES[1];

	const [timeseries, joinTimeseries] = await Promise.all([
		api.getModelTimeseries(params.slug, startTimestamp, endTimestamp),
		api.getJoinTimeseries(params.slug, startTimestamp, endTimestamp)
	]);

	return {
		timeseries,
		joinTimeseries,
		dateRange
	};
};

import type { PageServerLoad } from './$types';
import * as api from '$lib/api/api';
import type { TimeSeriesResponse, JoinTimeSeriesResponse } from '$lib/types/Model/Model';
import { parseDateRangeParams } from '$lib/util/date-ranges';

export const load: PageServerLoad = async ({
	params,
	url
}): Promise<{
	timeseries: TimeSeriesResponse;
	joinTimeseries: JoinTimeSeriesResponse;
}> => {
	const dateRange = parseDateRangeParams(url.searchParams);

	const [timeseries, joinTimeseries] = await Promise.all([
		api.getModelTimeseries(params.slug, dateRange.startTimestamp, dateRange.endTimestamp),
		api.getJoinTimeseries(params.slug, dateRange.startTimestamp, dateRange.endTimestamp)
	]);

	return {
		timeseries,
		joinTimeseries
	};
};

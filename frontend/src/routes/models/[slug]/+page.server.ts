import type { PageServerLoad } from './$types';
import * as api from '$lib/api/api';
import type { TimeSeriesResponse, JoinTimeSeriesResponse, Model } from '$lib/types/Model/Model';
import { parseDateRangeParams } from '$lib/util/date-ranges';

export const load: PageServerLoad = async ({
	params,
	url
}): Promise<{
	timeseries: TimeSeriesResponse;
	joinTimeseries: JoinTimeSeriesResponse;
	model?: Model;
}> => {
	const dateRange = parseDateRangeParams(url.searchParams);

	const [timeseries, joinTimeseries, models] = await Promise.all([
		api.getModelTimeseries(params.slug, dateRange.startTimestamp, dateRange.endTimestamp),
		api.getJoinTimeseries(params.slug, dateRange.startTimestamp, dateRange.endTimestamp),
		await api.getModels() // todo eventually we will want to get a single model
	]);

	const modelToReturn = models.items.find((m) => m.name === params.slug);

	return {
		timeseries,
		joinTimeseries,
		model: modelToReturn
	};
};

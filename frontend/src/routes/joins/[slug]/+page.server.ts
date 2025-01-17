import type { PageServerLoad } from './$types';
import { Api } from '$lib/api/api';
import { parseDateRangeParams } from '$lib/util/date-ranges';
import { getMetricTypeFromParams } from '$lib/types/MetricType/MetricType';
import { getSortDirection } from '$lib/util/sort';
import { getJoinData } from '$routes/joins/[slug]/services/joins.service';
import { getJobTrackerData } from '$routes/joins/[slug]/services/jobTracker.service';

export const load: PageServerLoad = async ({ params, url, fetch }) => {
	const api = new Api({ fetch });
	const requestedDateRange = parseDateRangeParams(url.searchParams);
	const joinName = params.slug;
	const metricType = getMetricTypeFromParams(url.searchParams);
	const sortDirection = getSortDirection(url.searchParams, 'drift');

	const [joinData, jobTrackerData] = await Promise.all([
		getJoinData(api, joinName, requestedDateRange, metricType, sortDirection),
		getJobTrackerData()
	]);

	return {
		...joinData,
		jobTree: jobTrackerData.jobTree,
		dates: jobTrackerData.dates
	};
};

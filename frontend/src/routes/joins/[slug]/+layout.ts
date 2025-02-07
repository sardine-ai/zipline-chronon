import type { PageServerLoad } from './$types';
import { Api } from '$lib/api/api';
import { parseDateRangeParams } from '$lib/util/date-ranges';
import { getDriftMetricFromParams } from '$src/lib/util/drift-metric';
import { getJoinData } from '$routes/joins/[slug]/services/joins.service';
import { getJobTrackerData } from '$routes/joins/[slug]/services/jobTracker.service';

export const load: PageServerLoad = async ({ params, url, fetch }) => {
	const api = new Api({ fetch });
	const requestedDateRange = parseDateRangeParams(url.searchParams);
	const joinName = params.slug;
	const driftMetric = getDriftMetricFromParams(url.searchParams);

	const [joinData, jobTrackerData] = await Promise.all([
		getJoinData(api, joinName, requestedDateRange, driftMetric),
		// TODO: Move to `job-tracking` route instead of layout
		getJobTrackerData()
	]);

	return {
		...joinData,
		jobTree: jobTrackerData.jobTree,
		dates: jobTrackerData.dates
	};
};

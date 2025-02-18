import type { PageServerLoad } from './$types';
import { Api } from '$lib/api/api';
import { parseDateRangeParams } from '$lib/util/date-ranges';
import { getDriftMetricFromParams } from '$src/lib/util/drift-metric';
import { getJoinData } from '$routes/joins/[slug]/services/joins.service';

export const load: PageServerLoad = async ({ params, url, fetch }) => {
	const api = new Api({ fetch });
	const requestedDateRange = parseDateRangeParams(url.searchParams);
	const joinName = params.slug;
	const driftMetric = getDriftMetricFromParams(url.searchParams);

	const [joinData] = await Promise.all([
		getJoinData(api, joinName, requestedDateRange, driftMetric)
	]);

	return {
		...joinData
	};
};

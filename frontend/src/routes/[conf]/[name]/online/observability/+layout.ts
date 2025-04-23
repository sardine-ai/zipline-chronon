import { Api } from '$src/lib/api/api';
import { parseDateRangeParams } from '$src/lib/params/date-ranges';
import { getDriftMetricFromParams } from '$src/lib/params/drift-metric';
import { type IJoinDriftResponseArgs } from '$lib/types/codegen';
import { DEMO_DATE_START, DEMO_DATE_END } from '$src/lib/constants/common';
import { getOffset } from '$src/lib/params/offset';

export async function load({ url, params }) {
	const requestedDateRange = parseDateRangeParams(url.searchParams);
	const driftMetric = getDriftMetricFromParams(url.searchParams);
	const offset = getOffset(url.searchParams);

	const confName = params.name;

	let dateRange = {
		...requestedDateRange,
		isUsingFallback: false
	};

	const api = new Api({ fetch });

	let joinDrift: IJoinDriftResponseArgs | undefined = undefined;
	try {
		// Try with requested date range first
		joinDrift = await api.getJoinDrift({
			name: confName,
			startTs: dateRange.startTimestamp,
			endTs: dateRange.endTimestamp,
			algorithm: driftMetric,
			offset: offset + 'd'
		});

		// If empty data is results, use fallback date range
		const useFallback = joinDrift.driftSeries.every((ds) => Object.keys(ds).length <= 1); // Only `key` returned on results
		if (useFallback) {
			dateRange = {
				...dateRange,
				startTimestamp: DEMO_DATE_START.valueOf(),
				endTimestamp: DEMO_DATE_END.valueOf(),
				isUsingFallback: true
			};

			joinDrift = await api.getJoinDrift({
				name: confName,
				startTs: dateRange.startTimestamp,
				endTs: dateRange.endTimestamp,
				algorithm: driftMetric
			});
		}
	} catch {
		// Drift series data not available for join
	}

	return {
		joinDrift,
		driftMetric,
		dateRange
	};
}

import { Api } from '$src/lib/api/api';
import { parseDateRangeParams } from '$lib/util/date-ranges';
import { getDriftMetricFromParams } from '$src/lib/util/drift-metric';
import { type IJoinDriftResponse } from '$lib/types/codegen';

const FALLBACK_START_TS = 1672531200000; // 2023-01-01
const FALLBACK_END_TS = 1677628800000; // 2023-03-01

export async function load({ url, params }) {
	const requestedDateRange = parseDateRangeParams(url.searchParams);
	const driftMetric = getDriftMetricFromParams(url.searchParams);

	const confName = params.name;

	let dateRange = {
		...requestedDateRange,
		isUsingFallback: false
	};

	const api = new Api({ fetch });

	let joinDrift: IJoinDriftResponse | undefined = undefined;
	try {
		// Try with requested date range first
		joinDrift = await api.getJoinDrift({
			name: confName,
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

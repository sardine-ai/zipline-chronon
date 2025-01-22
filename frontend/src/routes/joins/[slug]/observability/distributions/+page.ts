import { Api } from '$lib/api/api';

export const load = async ({ parent, fetch }) => {
	const { joinTimeseries, dateRange } = await parent();
	const api = new Api({ fetch });

	// Get all unique feature names across all groups
	const allFeatures = Array.from(
		new Set(joinTimeseries.items.flatMap((group) => group.items.map((item) => item.feature)))
	);

	// Fetch percentile data for each feature
	const distributionsPromise = Promise.all(
		allFeatures.map((featureName) =>
			api.getFeatureTimeseries({
				joinId: joinTimeseries.name,
				featureName,
				startTs: dateRange.startTimestamp,
				endTs: dateRange.endTimestamp,
				granularity: 'percentile',
				metricType: 'drift',
				metrics: 'value',
				offset: '1D',
				algorithm: 'psi'
			})
		)
	).then((responses) => {
		return responses.filter((response) => response.isNumeric);
	});

	return {
		distributionsPromise
	};
};

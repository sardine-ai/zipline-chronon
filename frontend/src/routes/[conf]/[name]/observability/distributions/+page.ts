import { Api } from '$lib/api/api';

export const load = async ({ parent, fetch }) => {
	const { conf, joinDrift, dateRange } = await parent();
	const api = new Api({ fetch });

	const confName = conf?.metaData?.name?.replace('/', '.') ?? 'Unknown';
	const columnNames = joinDrift?.driftSeries.map((ds) => ds.key?.column ?? 'Unknown') ?? [];

	// Fetch percentile data for each column
	// TODO: Ignoring errored columns but maybe we should show them in an error state instead
	const distributionsPromise = Promise.allSettled(
		columnNames.map((columnName) =>
			api.getColumnSummary({
				name: confName,
				columnName: columnName,
				startTs: dateRange.startTimestamp,
				endTs: dateRange.endTimestamp
			})
		)
	).then((responses) => {
		return responses
			.filter((response) => response.status === 'fulfilled')
			.map((response) => response.value)
			.filter((value) => value.percentiles);
	});

	return {
		distributionsPromise
	};
};

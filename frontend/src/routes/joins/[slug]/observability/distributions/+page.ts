import { Api } from '$lib/api/api';

export const load = async ({ parent, fetch }) => {
	const { joinDrift, dateRange } = await parent();
	const api = new Api({ fetch });

	const joinName = joinDrift.driftSeries[0].key?.nodeName?.replace('/', '.') ?? 'Unknown';
	const columnNames = joinDrift.driftSeries.map((ds) => ds.key?.column ?? 'Unknown');

	// Fetch percentile data for each column
	// TODO: Ignoring errored columns but maybe we should show them in an error state instead
	const distributionsPromise = Promise.allSettled(
		columnNames.map((columnName) =>
			api.getColumnSummary({
				name: joinName,
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

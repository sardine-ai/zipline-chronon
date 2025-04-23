import type { ComponentProps } from 'svelte';
import { flatRollup, randomUniform } from 'd3';
import { parseISO } from 'date-fns';

import { Api } from '$lib/api/api';
import { generateTileSummarySeriesData } from '$src/lib/util/test-data/tile-series';
import { DEMO_DATE_START, DEMO_DATE_END } from '$src/lib/constants/common';
import type CustomLineChart from '$src/lib/components/charts/CustomLineChart.svelte';
import { getOffset } from '$src/lib/params/offset.js';

export async function load({ url, parent, fetch }) {
	const { conf } = await parent();
	const api = new Api({ fetch });

	const startDate = DEMO_DATE_START;
	const endDate = DEMO_DATE_END;
	const offset = getOffset(url.searchParams);

	const confName = conf?.metaData?.name ?? 'Unknown';

	// TODO: Remove once we have a way to get data quality metrics
	// Hack to get some tile series keys to generate data
	const joinDrift = await api.getJoinDrift({
		name: confName,
		startTs: 0,
		endTs: 0,
		offset: offset + 'd'
	});
	const _tileSeriesKeys = joinDrift.driftSeries.map((ds) => ds.key!);
	// Remove duplicates - https://linear.app/zipline-ai/issue/ZIP-445/why-are-there-duplicate-nodes
	const tileSeriesKeys = flatRollup(
		_tileSeriesKeys,
		(values) => values[0],
		(d) => d.column
	).map((d) => d[1]);
	// const columnNames = joinDrift?.driftSeries.map((ds) => ds.key?.column ?? 'Unknown') ?? [];

	// Fetch percentile data for each column
	// TODO: Ignoring errored columns but maybe we should show them in an error state instead
	const columnSummaries = await Promise.allSettled(
		tileSeriesKeys.map((key) =>
			api.getColumnSummary({
				name: confName,
				columnName: key.column ?? 'Unknown',
				startTs: startDate.valueOf(),
				endTs: endDate.valueOf()
			})
		)
	).then((responses) => {
		return responses
			.filter((response) => response.status === 'fulfilled')
			.map((response) => response.value);
	});

	// Override API data with improved generated data examples
	for (const [i, columnSummary] of columnSummaries.entries()) {
		const points = columnSummary.count?.length ?? 1000; // Match demo data

		// Default generated data
		const nullRatio = randomUniform(0, 0.2)() * (i / columnSummaries.length);
		// eslint-disable-next-line prefer-const
		let generatedData = generateTileSummarySeriesData({
			key: columnSummary.key!,
			startDate: startDate,
			endDate: endDate,
			points,
			normal: {
				total: [10_000, 10_000],
				nullRatio: [nullRatio, nullRatio + randomUniform(0, 0.2)()]
			},
			anomalies: [
				// Running, no data
				{
					startDate: parseISO('2023-02-07'),
					endDate: endDate, // ongoing
					total: null,
					nullRatio: [0, 0]
				}
			]
		});

		// Specific use cases
		if (columnSummary.key!.groupName === 'dim_merchant') {
			// if (columnSummary.key!.column === 'dim_merchant_account_age') {
			// 	generatedData = generateTileSummarySeriesData({
			// 		key: columnSummary.key!,
			// 		startDate: startDate,
			// 		endDate: endDate,
			// 		points,
			// 		normal: { total: [10_000, 10_000], nullRatio: [0, 0] },
			// 		anomalies: [
			// 			// 100% null
			// 			{
			// 				startDate: parseISO('2023-01-15'),
			// 				endDate: parseISO('2023-01-16'),
			// 				total: [10_000, 10_000],
			// 				nullRatio: [1, 1]
			// 			},
			// 			// No records
			// 			{
			// 				startDate: parseISO('2023-02-01'),
			// 				endDate: parseISO('2023-02-03'),
			// 				total: [0, 0],
			// 				nullRatio: [0, 0]
			// 			},
			// 			// All null new records
			// 			{
			// 				startDate: parseISO('2023-02-26'),
			// 				endDate: endDate, // ongoing
			// 				total: [12_000, 12_000],
			// 				nullRatio: [0.1667, 0.1667]
			// 			}
			// 		]
			// 	});
			// } else {
			// generatedData = generateTileSummarySeriesData({
			// 	key: columnSummary.key!,
			// 	startDate: startDate,
			// 	endDate: endDate,
			// 	points,
			// 	normal: {
			// 		total: [9_000, 10_000],
			// 		nullRatio: [0.2, 0.3]
			// 	}
			// });
			// }
		} else if (columnSummary.key!.groupName === 'dim_user') {
			// generatedData = generateTileSummarySeriesData({
			// 	key: columnSummary.key!,
			// 	startDate: startDate,
			// 	endDate: endDate,
			// 	points,
			// 	normal: { total: [10_000, 11_000], nullRatio: [0.2, 0.3] },
			// 	anomalies: []
			// });
		} else if (columnSummary.key!.groupName === 'txn_by_merchant') {
			// generatedData = generateTileSummarySeriesData({
			// 	key: columnSummary.key!,
			// 	startDate: startDate,
			// 	endDate: endDate,
			// 	points,
			// 	normal: { total: [10_000, 11_000], nullRatio: [0.2, 0.3] },
			// 	anomalies: []
			// });
		} else if (columnSummary.key!.groupName === 'txn_by_user') {
			// generatedData = generateTileSummarySeriesData({
			// 	key: columnSummary.key!,
			// 	startDate: startDate,
			// 	endDate: endDate,
			// 	points,
			// 	normal: { total: [10_000, 11_000], nullRatio: [0.2, 0.2] },
			// 	anomalies: []
			// });
		}

		// Override the count and nullCount values with the generated data
		columnSummary.count = generatedData.count;
		columnSummary.nullCount = generatedData.nullCount;
	}

	const rowCountSummarySeries = generateTileSummarySeriesData({
		key: tileSeriesKeys[0], // just something to use
		startDate: startDate,
		endDate: endDate,
		points: columnSummaries[0].timestamps?.length ?? 1000,
		normal: { total: [10_000, 11_000], nullRatio: [0, 0] },
		anomalies: [
			// 10x spike
			{
				startDate: parseISO('2023-02-05'),
				endDate: parseISO('2023-02-07'),
				total: [10_000 * 10, 10_100 * 10],
				nullRatio: [0, 0]
			},
			// Running, no data
			{
				startDate: parseISO('2023-02-07'),
				endDate: endDate, // ongoing
				total: null,
				nullRatio: [0, 0]
			}
		]
	});

	return {
		columnSummaries,
		rowCountSummarySeries, // for demo purposes only

		// TODO: Generate from alerts/etc
		annotations: [
			// Show on "Row count"
			{
				type: 'range',
				x: [parseISO('2023-02-05'), parseISO('2023-02-07')],
				gradient: {
					stops: ['hsl(40 100% 25% / 50%)', 'transparent'],
					vertical: true
				},
				layer: 'below'
			},
			{
				type: 'range',
				x: [parseISO('2023-02-07'), endDate], // ongoing
				pattern: {
					size: 8,
					lines: {
						rotate: -45,
						opacity: 0.2
					}
				}
			},
			{
				type: 'point',
				label: '!',
				x: parseISO('2023-02-05'),
				classes: {
					circle: 'fill-[hsl(40_100%_50%)]'
				}
			}

			// Show on null ratio charts
			// {
			// 	type: 'range',
			// 	x: [parseISO('2023-01-15'), parseISO('2023-01-16')],
			// 	seriesKey: 'dim_merchant_account_age',
			// 	gradient: {
			// 		stops: ['hsl(40 100% 25% / 50%)', 'transparent'],
			// 		vertical: true
			// 	},
			// 	layer: 'below'
			// },
			// {
			// 	type: 'range',
			// 	x: [parseISO('2023-02-01'), parseISO('2023-02-03')],
			// 	seriesKey: 'dim_merchant_account_age',
			// 	gradient: {
			// 		stops: ['hsl(40 100% 25% / 50%)', 'transparent'],
			// 		vertical: true
			// 	},
			// 	layer: 'below'
			// },
			// {
			// 	type: 'range',
			// 	x: [parseISO('2023-02-26'), endDate], // ongoing
			// 	seriesKey: 'dim_merchant_account_age',
			// 	gradient: {
			// 		stops: ['hsl(40 100% 25% / 50%)', 'transparent'],
			// 		vertical: true
			// 	},
			// 	layer: 'below'
			// }
		] as NonNullable<ComponentProps<typeof CustomLineChart>['annotations']>
	};
}

import type { ComponentProps } from 'svelte';
import { parseISO } from 'date-fns';

import { Api } from '$lib/api/api';
import { generateTileDriftSeriesData } from '$src/lib/util/test-data/tile-series';
import { DEMO_DATE_START, DEMO_DATE_END } from '$src/lib/constants/common';
import { getDriftMetricFromParams } from '$src/lib/params/drift-metric.js';
import type CustomLineChart from '$src/lib/components/charts/CustomLineChart.svelte';
import { getOffset } from '$src/lib/params/offset';
import { randomUniform } from 'd3';

export async function load({ url, parent, fetch }) {
	const { conf } = await parent();
	const api = new Api({ fetch });

	const driftMetric = getDriftMetricFromParams(url.searchParams);

	const startDate = DEMO_DATE_START;
	const endDate = DEMO_DATE_END;
	const offset = getOffset(url.searchParams);

	const confName = conf?.metaData?.name ?? 'Unknown';

	// TODO: Remove once we have a way to get data quality metrics
	// Hack to get some tile series keys to generate data
	const joinDrift = await api.getJoinDrift({
		name: confName,
		startTs: startDate.valueOf(),
		endTs: endDate.valueOf(),
		algorithm: driftMetric,
		offset: offset + 'd'
	});

	// Override API data with improved generated data examples
	for (const [i, driftSeries] of joinDrift.driftSeries.entries()) {
		const points = driftSeries.timestamps?.length ?? 1000; // Match demo data

		// Default generated data
		const nullRatio = randomUniform(0, 0.02)() * (i / joinDrift.driftSeries.length);
		// eslint-disable-next-line prefer-const
		let generatedData = generateTileDriftSeriesData({
			key: driftSeries.key!,
			startDate: startDate,
			endDate: endDate,
			points,
			normal: {
				total: [10_000, 10_000],
				nullRatio: [nullRatio, nullRatio + randomUniform(0, 0.02)()]
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
		// if (driftSeries.key!.groupName === 'dim_merchant') {
		// 	generatedData = generateTileDriftSeriesData({
		// 		key: driftSeries.key!,
		// 		startDate: startDate,
		// 		endDate: endDate,
		// 		points,
		// 		normal: { total: [10_000, 10_000], nullRatio: [0, 0] },
		// 		anomalies: [
		// 			// 100% null
		// 			{
		// 				startDate: parseISO('2023-01-29'),
		// 				endDate: parseISO('2023-01-30'),
		// 				total: [10_000, 10_000],
		// 				nullRatio: [1, 1]
		// 			},
		// 			// No records
		// 			{
		// 				startDate: parseISO('2023-02-05'),
		// 				endDate: parseISO('2023-02-07'),
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
		// } else if (driftSeries.key!.groupName === 'dim_user') {
		// 	generatedData = generateTileDriftSeriesData({
		// 		key: driftSeries.key!,
		// 		startDate: startDate,
		// 		endDate: endDate,
		// 		points,
		// 		normal: { total: [10_000, 11_000], nullRatio: [0.2, 0.3] },
		// 		anomalies: []
		// 	});
		// } else if (driftSeries.key!.groupName === 'txn_by_merchant') {
		// 	generatedData = generateTileDriftSeriesData({
		// 		key: driftSeries.key!,
		// 		startDate: startDate,
		// 		endDate: endDate,
		// 		points,
		// 		normal: { total: [10_000, 11_000], nullRatio: [0.2, 0.3] },
		// 		anomalies: []
		// 	});
		// } else if (driftSeries.key!.groupName === 'txn_by_user') {
		// 	generatedData = generateTileDriftSeriesData({
		// 		key: driftSeries.key!,
		// 		startDate: startDate,
		// 		endDate: endDate,
		// 		points,
		// 		normal: { total: [10_000, 11_000], nullRatio: [0.2, 0.2] },
		// 		anomalies: []
		// 	});
		// }

		// Override the count and nullCount values with the generated data
		driftSeries.countChangePercentSeries = generatedData.countChangePercentSeries as number[];
		driftSeries.nullRatioChangePercentSeries =
			generatedData.nullRatioChangePercentSeries as number[];
	}

	// Create specific drift series to use for row count (demo)
	const firstCountDriftSeries = joinDrift.driftSeries[0];
	const rowCountDriftSeries = generateTileDriftSeriesData({
		key: firstCountDriftSeries.key!, // just something to use
		startDate: startDate,
		endDate: endDate,
		points: firstCountDriftSeries.timestamps?.length ?? 1000,
		normal: { total: [0.01, 0.2], nullRatio: [0, 0] },
		anomalies: [
			// 10x spike
			{
				startDate: parseISO('2023-02-05'),
				endDate: parseISO('2023-02-07'),
				total: [13, 15],
				nullRatio: [0, 0]
			},
			// Running, no data
			{
				startDate: parseISO('2023-02-07'),
				endDate: endDate, // ongoing
				total: null,
				nullRatio: [0, 0]
			}
		],
		totalType: 'decimal'
	});

	return {
		joinDrift,
		driftMetric,
		rowCountDriftSeries,
		dateRange: { start: startDate, end: endDate },

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
			},

			// Show on drift values
			{
				type: 'range',
				x: [parseISO('2023-01-27T19:00'), parseISO('2023-02-02T19:00')],
				seriesKey: 'dim_merchant_country',
				gradient: {
					stops: ['hsl(40 100% 25% / 50%)', 'transparent'],
					vertical: true
				},
				layer: 'below'
			},
			{
				type: 'point',
				label: '!',
				x: parseISO('2023-01-27T19:00'),
				seriesKey: 'dim_merchant_country',
				classes: {
					circle: 'fill-[hsl(40_100%_50%)]'
				}
			}
		] as NonNullable<ComponentProps<typeof CustomLineChart>['annotations']>
	};
}

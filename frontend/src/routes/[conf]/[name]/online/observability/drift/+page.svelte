<script lang="ts">
	import { onMount } from 'svelte';
	import { Button, Drawer } from 'svelte-ux';
	import { BarChart, PieChart, type SeriesData } from 'layerchart';
	import type { DomainType } from 'layerchart/utils/scales.svelte';
	import { entries, sort } from '@layerstack/utils';
	import { cls } from '@layerstack/tailwind';
	import { rollups } from 'd3';
	import { parseISO, sub, type Duration } from 'date-fns';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import type { ITileSummarySeriesArgs } from '$src/lib/types/codegen';
	import { Api } from '$lib/api/api';
	import { DRIFT_METRIC_SCALES } from '$lib/params/drift-metric';
	import CustomLineChart from '$lib/components/charts/CustomLineChart.svelte';
	import PercentileLineChart from '$lib/components/charts/PercentileLineChart.svelte';
	import {
		type DateValue,
		barChartProps,
		pieChartProps,
		tooltipProps,
		xAxisProps,
		yAxisProps,
		textClass
	} from '$lib/components/charts/common';
	import { isMacOS } from '$src/lib/util/browser';
	import { getSortDirection } from '$src/lib/params/sort';
	import { NULL_VALUE } from '$src/lib/constants/common';
	import { page } from '$app/state';
	import { pushState } from '$app/navigation';
	import { transformSeries } from '$src/lib/components/charts/utils';
	import IconXMark from '~icons/heroicons/x-mark';
	import { generateTileSummarySeriesData } from '$src/lib/util/test-data/tile-series.js';
	import DriftMetricToggle from '$src/lib/components/DriftMetricToggle.svelte';
	import SortButton from '$src/lib/components/SortButton.svelte';
	import ResetZoomButton from '$src/lib/components/ResetZoomButton.svelte';
	import { isZoomed, resetZoom, shared } from '../../../shared.svelte.js';

	const api = new Api();

	const { data } = $props();

	const sortDirection = $derived(getSortDirection(page.url.searchParams, 'drift'));

	const baselineOffset: Duration = { days: 7 };

	// Group by group name and sort groups and columns
	const driftSeriesByGroupName = $derived(
		sort(
			rollups(
				data.joinDrift?.driftSeries ?? [],
				(values) => sort(values, (d) => d.key.column, 'asc'),
				(d) => d.key?.groupName ?? 'Unknown'
			),
			(d) => d[0],
			sortDirection
		)
	);

	const driftMetricDomain = $derived.by(() => {
		const scale = DRIFT_METRIC_SCALES[data.driftMetric];
		return [scale.min, scale.max];
	});

	let selectedSeriesPoint = $state<{ series: SeriesData<any, any>; data: DateValue } | null>(null);

	let lockedTooltip = $state(false);

	let columnSummaryData: ITileSummarySeriesArgs | null = $state(null);
	let columnSummaryBaselineData: ITileSummarySeriesArgs | null = $state(null);

	async function selectSeriesPoint(seriesPoint: typeof selectedSeriesPoint) {
		selectedSeriesPoint = seriesPoint;

		if (seriesPoint) {
			try {
				const joinName = data.conf?.metaData?.name ?? 'Unknown';
				const columnName = seriesPoint.series.key.toString();

				// TODO: Add loading and error states
				const [_columnSummaryData, _columnSummaryBaselineData] = await Promise.all([
					api.getColumnSummary({
						name: joinName,
						columnName: columnName,
						startTs: data.dateRange.startTimestamp,
						endTs: data.dateRange.endTimestamp
					}),
					api.getColumnSummary({
						name: joinName,
						columnName: columnName,
						startTs: Number(sub(new Date(data.dateRange.startTimestamp), baselineOffset)),
						endTs: Number(sub(new Date(data.dateRange.endTimestamp), baselineOffset))
					})
				]);

				columnSummaryData = _columnSummaryData;
				columnSummaryBaselineData = _columnSummaryBaselineData;
			} catch (error) {
				console.error('Error fetching data:', error);
				columnSummaryData = null;
				columnSummaryBaselineData = null;
			}
		}
	}

	onMount(() => {
		setTimeout(() => {
			if (page.url.searchParams.get('node') && page.url.searchParams.get('timestamp')) {
				const nodeId = page.url.searchParams.get('node');
				const timestamp = Number(page.url.searchParams.get('timestamp'));

				const series = transformSeries(
					data.joinDrift?.driftSeries ?? [],
					(s) => s.percentileDriftSeries ?? s.histogramDriftSeries ?? []
				).find((s) => s.key === nodeId);

				if (series) {
					const point = series?.data.find((d) => Number(d.date) === timestamp);

					if (point) {
						selectSeriesPoint({
							series,
							data: point
						} as typeof selectedSeriesPoint);
					}
				}
			}
		});
	});

	$effect(() => {
		if (page.state.selectedSeriesPoint) {
			selectSeriesPoint(page.state.selectedSeriesPoint);
		} else {
			selectSeriesPoint(null);
		}
	});

	const testDataConfigs = [
		{
			key: {
				column: 'txn_by_merchant_transaction_amount_count_7d'
			},
			normal: { total: [700, 800], nullRatio: [0.1, 0.2] },
			anomalies: [
				{
					startDate: parseISO('2023-01-25'),
					endDate: parseISO('2023-01-31'),
					total: [800, 900],
					nullRatio: [0.8, 0.9]
				},
				{
					startDate: parseISO('2023-02-12'),
					endDate: new Date(data.dateRange.endTimestamp), // ongoing
					total: [800, 800],
					nullRatio: [0.8, 0.9]
				}
			]
		},
		{
			key: {
				column: 'txn_by_merchant_transaction_amount_count_1d'
			},
			normal: { total: [1000, 1200], nullRatio: [0.1, 0.2] },
			anomalies: [
				{
					startDate: parseISO('2023-01-08'),
					endDate: parseISO('2023-01-09'),
					total: [1000, 1000],
					nullRatio: [1, 1]
				}
			]
		},
		{
			key: {
				column: 'dim_merchant_zipcode'
			},
			normal: { total: [900, 1000], nullRatio: [0.8, 0.9] },
			anomalies: []
		}
	];

	const testData = testDataConfigs.map((config) =>
		generateTileSummarySeriesData({
			...config,
			startDate: new Date(data.dateRange.startTimestamp),
			endDate: new Date(data.dateRange.endTimestamp),
			points: data.joinDrift?.driftSeries[0].timestamps?.length ?? 500
		})
	);
</script>

<div
	class="sticky top-0 z-20 bg-neutral-50 dark:bg-neutral-100 border-b border-border -mx-8 py-2 px-8 border-l"
>
	<div class="flex items-center justify-end gap-3">
		<DriftMetricToggle />
		<SortButton context="drift" />
	</div>
</div>

{#if driftSeriesByGroupName.length === 0}
	<div class="mt-6 bg-destructive/10 border border-destructive/50 p-4 rounded font-medium">
		Drift unavailable
	</div>
{:else}
	<div class="grid gap-4 mt-4">
		<div class="border rounded-md divide-y">
			<CollapsibleSection label="Null rate" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(
							testData,
							(s) =>
								s.count?.map((c, i) => {
									const count = c as number;
									const nullCount = (s.nullCount?.[i] as number) ?? 0;
									return nullCount / (count + nullCount);
									// return nullCount / count;
								}) ?? []
						)}
						xDomain={shared.xDomain}
						yDomain={[0, 1]}
						format="percent"
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							shared.xDomain = detail.xDomain;
						}}
						{lockedTooltip}
						thresholds={[{ label: 'LIMIT', value: 0.75 }]}
						annotations={testDataConfigs.flatMap((c) => [
							...c.anomalies.map((a) => ({
								type: 'point' as const,
								x: a.startDate,
								seriesKey: c.key.column
							})),
							...c.anomalies.map((a) => ({
								type: 'range' as const,
								x: [a.startDate, a.endDate],
								seriesKey: c.key.column
							}))
						])}
					/>
				</div>
			</CollapsibleSection>

			<CollapsibleSection label="Row count" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(
							testData,
							(s) =>
								s.count?.map((c, i) => {
									const count = c as number;
									const nullCount = (s.nullCount?.[i] as number) ?? 0;
									return count + nullCount;
								}) ?? []
						)}
						xDomain={shared.xDomain}
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							shared.xDomain = detail.xDomain;
						}}
						{lockedTooltip}
						format="metric"
					/>
				</div>
			</CollapsibleSection>

			<!-- <CollapsibleSection label="Null rate" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(data.joinDrift?.driftSeries ?? [], (s) => {
							// console.log({ s });
							// Not all drift series have `nullRatioChangePercentSeries`
							return s.nullRatioChangePercentSeries ?? [];
						})}
						{xDomain}
						format="percent"
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							xDomain = detail.xDomain;
						}}
						{lockedTooltip}
					/>
				</div>
			</CollapsibleSection>

			<CollapsibleSection label="Row change" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(data.joinDrift?.driftSeries ?? [], (s) => {
							// console.log({ s });
							// Not all drift series have `countChangePercentSeries`
							return s.countChangePercentSeries ?? [];
						})}
						{xDomain}
						format="percent"
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							xDomain = detail.xDomain;
						}}
						{lockedTooltip}
					/>
				</div>
			</CollapsibleSection> -->
		</div>

		<div class="border rounded-md mt-4 divide-y px-2">
			{#each driftSeriesByGroupName as [groupName, values] (groupName)}
				<CollapsibleSection label={groupName} open class="py-4 text-sm">
					<div class="h-[280px]">
						<CustomLineChart
							series={transformSeries(
								values,
								(s) => s.percentileDriftSeries ?? s.histogramDriftSeries ?? []
							)}
							yDomain={driftMetricDomain}
							onPointClick={(_, { series, data }) => {
								const url = new URL(window.location.href);
								url.searchParams.set('node', series.key.toString());
								url.searchParams.set(
									'timestamp',
									new Date((data as DateValue).date).getTime().toString()
								);
								pushState(url.toString(), {
									selectedSeriesPoint: { series, data: data as DateValue }
								});
							}}
							onItemClick={({
								series,
								data
							}: {
								series: SeriesData<any, any>;
								data: DateValue;
							}) => {
								selectSeriesPoint({ series, data });
							}}
							xDomain={shared.xDomain}
							onBrushEnd={(detail: { xDomain?: DomainType }) => {
								shared.xDomain = detail.xDomain;
							}}
							{lockedTooltip}
						/>
					</div>
				</CollapsibleSection>
			{/each}
		</div>
	</div>
{/if}

<!-- Add extra space at bottom of page for tooltip -->
<div class="h-[300px]"></div>

<Drawer
	open={selectedSeriesPoint != null}
	on:close={() => {
		if (selectedSeriesPoint) {
			history.back();
		}
	}}
	let:close
	classes={{
		root: 'border-l'
	}}
>
	<div class="w-[1000px] max-w-[90vw]">
		<header class="sticky top-0 z-10 bg-surface-100 border-b py-4 px-6 mb-3">
			<span
				class="mb-4 ml-2 text-xl font-medium flex items-center gap-2 w-fit"
				role="presentation"
				onmouseenter={() => {
					/*highlightSeries(selectedSeries ?? '', dialogGroupChart, 'highlight')*/
				}}
				onmouseleave={() => {
					/*highlightSeries(selectedSeries ?? '', dialogGroupChart, 'downplay')*/
				}}
			>
				<div
					class="w-3 h-3 rounded-full"
					style:background-color={selectedSeriesPoint?.series.color}
				></div>
				<div>
					{selectedSeriesPoint?.series.key}
				</div>
			</span>

			<div class="flex items-center justify-end gap-3">
				{#if isZoomed()}
					<ResetZoomButton onClick={resetZoom} />
				{/if}
			</div>

			<Button class="absolute top-2 right-2 w-8 h-8 p-0" on:click={() => close()}>
				<IconXMark />
				<span class="sr-only">Close</span>
			</Button>
		</header>

		<div class="grid gap-6 px-6 overflow-y-auto">
			{#if selectedSeriesPoint}
				{@const [groupName, values] =
					driftSeriesByGroupName.find(([_, values]) =>
						values.some((value) => value.key?.column === selectedSeriesPoint?.series.key)
					) ?? []}

				{#if groupName && values}
					<CollapsibleSection label={groupName} open>
						<div class="h-[280px]">
							<CustomLineChart
								series={transformSeries(
									values,
									(s) => s.percentileDriftSeries ?? s.histogramDriftSeries ?? []
								)}
								yDomain={driftMetricDomain}
								annotations={[
									{
										type: 'line',
										x: selectedSeriesPoint?.data.date,
										y: selectedSeriesPoint?.data.value,
										seriesKey: selectedSeriesPoint?.series.key
									}
								]}
								onPointClick={(_e: MouseEvent, { series, data }) => {
									const url = new URL(window.location.href);
									url.searchParams.set('node', series.key.toString());
									url.searchParams.set(
										'timestamp',
										new Date((data as DateValue).date).getTime().toString()
									);
									pushState(url.toString(), {
										selectedSeriesPoint: { series, data: data as DateValue }
									});
								}}
								onItemClick={({
									series,
									data
								}: {
									series: SeriesData<any, any>;
									data: DateValue;
								}) => {
									selectSeriesPoint({ series, data });
								}}
								xDomain={shared.xDomain}
								onBrushEnd={(detail: { xDomain?: DomainType }) => {
									shared.xDomain = detail.xDomain;
								}}
								{lockedTooltip}
							/>
						</div>
					</CollapsibleSection>
				{/if}
			{/if}

			{#if columnSummaryData?.percentiles}
				<CollapsibleSection label="Percentiles" open>
					<div class="h-[230px]">
						<PercentileLineChart
							data={columnSummaryData!}
							xDomain={shared.xDomain}
							onBrushEnd={(detail: { xDomain?: DomainType }) => {
								shared.xDomain = detail.xDomain;
							}}
							{lockedTooltip}
						/>
					</div>
				</CollapsibleSection>
			{/if}

			{#if columnSummaryData?.timestamps && Object.keys(columnSummaryData?.histogram ?? {}).length > 0}
				{@const timestamp = Number(selectedSeriesPoint?.data.date)}
				{@const timestampIndex = columnSummaryData.timestamps.findIndex(
					(ts) => (ts as unknown as number) === timestamp
				)}
				{@const currentData = entries(
					columnSummaryData.histogram as unknown as Record<string, number[]>
				).map(([key, values]) => {
					const value = values[timestampIndex];
					return {
						label: key,
						value: value === NULL_VALUE ? null : value
					};
				})}
				{@const baselineData = entries(
					(columnSummaryBaselineData?.histogram ?? {}) as unknown as Record<string, number[]>
				).map(([key, values]) => {
					const value = values[timestampIndex];
					return {
						label: key,
						value: value === NULL_VALUE ? null : value
					};
				})}
				<CollapsibleSection label="Data Distribution" open>
					<div class="h-[230px]">
						<BarChart
							x="label"
							y="value"
							series={[
								{
									key: 'baseline',
									data: baselineData,
									color: '#2976E6' // TODO: copied from ECharts defaults
								},
								{
									key: 'current',
									data: currentData,
									color: '#3DDC91' // TODO: copied from ECharts defaults
								}
							]}
							seriesLayout="group"
							groupPadding={0.1}
							bandPadding={0.1}
							{...barChartProps}
							props={{
								yAxis: { ...yAxisProps },
								xAxis: { ...xAxisProps },
								tooltip: { ...tooltipProps, hideTotal: true },
								bars: {
									strokeWidth: 0
								}
							}}
						/>
					</div>
				</CollapsibleSection>
			{/if}

			{#if columnSummaryData?.timestamps && columnSummaryData?.nullCount}
				{@const timestamp = Number(selectedSeriesPoint?.data.date)}
				{@const timestampIndex = columnSummaryData.timestamps.findIndex(
					(ts) => (ts as unknown as number) === timestamp
				)}
				{@const currentNullValue = columnSummaryData.nullCount[timestampIndex] as unknown as number}
				{@const baselineNullValue = columnSummaryBaselineData?.nullCount?.[
					timestampIndex
				] as unknown as number}

				<CollapsibleSection label="Null Ratio" open>
					<div class="grid grid-cols-2 gap-3">
						{#each [{ label: 'Baseline', value: baselineNullValue }, { label: 'Current', value: currentNullValue }] as c}
							<!-- TODO: Remove mockNullValue once data is populated -->
							<!-- {@const mockNullValue = Math.random() * 100} -->
							<div class="grid gap-2">
								<div class="h-[230px]">
									<!--  TODO: colors copied from ECharts defaults -->
									<PieChart
										data={[
											{
												label: 'Null Value Percentage',
												value: c.value
												// value: mockNullValue
											},
											{
												label: 'Non-null Value Percentage',
												value: 100 - c.value
												// value: 100 - mockNullValue
											}
										]}
										key="label"
										value="value"
										cRange={['#2976E6', '#3DDC91']}
										{...pieChartProps}
									/>
								</div>
								<div class={cls('text-center', textClass)}>
									{c.label}
								</div>
							</div>
						{/each}
					</div>
				</CollapsibleSection>
			{/if}
		</div>
	</div>
</Drawer>

<svelte:window
	onkeydown={(e) => {
		if (isMacOS() ? e.metaKey : e.ctrlKey) {
			lockedTooltip = true;
		}
	}}
	onkeyup={(e) => {
		if (isMacOS() ? !e.metaKey : !e.ctrlKey) {
			lockedTooltip = false;
		}
	}}
/>

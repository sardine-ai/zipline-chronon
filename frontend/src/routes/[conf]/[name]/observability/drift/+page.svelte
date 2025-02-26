<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { BarChart, PieChart } from 'layerchart';
	import type { DomainType } from 'layerchart/utils/scales';
	import { entries, sort } from '@layerstack/utils';
	import { rollups } from 'd3';
	import { sub, type Duration } from 'date-fns';
	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import type { ITileSummarySeriesArgs } from '$src/lib/types/codegen';
	import { Api } from '$lib/api/api';
	import { Dialog, DialogContent, DialogHeader } from '$lib/components/ui/dialog';
	import { formatDate } from '$lib/util/format';
	import { DRIFT_METRIC_SCALES } from '$lib/util/drift-metric';
	import ChartControls from '$lib/components/ChartControls.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import ObservabilityNavTabs from '$routes/[conf]/[name]/observability/ObservabilityNavTabs.svelte';
	import FeaturesLineChart from '$lib/components/charts/FeaturesLineChart.svelte';
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
	import { cn } from '$src/lib/utils';
	import { isMacOS } from '$src/lib/util/browser';
	import { getSortDirection } from '$src/lib/util/sort';
	import { NULL_VALUE } from '$src/lib/constants/common';
	import { page } from '$app/state';
	import { pushState } from '$app/navigation';
	import { onMount } from 'svelte';
	import { getColumns, transformSeries } from '$lib/util/series';

	type FeaturesLineChartProps = ComponentProps<typeof FeaturesLineChart>;

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

	type SeriesItem = NonNullable<FeaturesLineChartProps['series']>[number];
	let selectedSeriesPoint = $state<{ series: SeriesItem; data: DateValue } | null>(null);

	let xDomain = $state<DomainType | undefined>(null);
	let isZoomed = $derived(xDomain != null);
	let lockedTooltip = $state(false);

	function resetZoom() {
		xDomain = null;
	}

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

				const series = driftSeriesByGroupName
					.flatMap(([_, groupValues]) => groupValues)
					.find((v) => v?.key?.column === nodeId);

				if (series && data.joinDrift) {
					const columns = getColumns(driftSeriesByGroupName.flatMap(([_, values]) => values));
					const transformedSeries = transformSeries(series, columns);
					const point = transformedSeries.data.find((d) => Number(d.date) === timestamp);

					if (point) {
						selectSeriesPoint({
							series: transformedSeries,
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
</script>

<div class="sticky top-0 z-20 bg-neutral-100 border-b border-border -mx-8 py-2 px-8 border-l">
	<ChartControls
		{isZoomed}
		onResetZoom={resetZoom}
		isUsingFallbackDates={data.dateRange.isUsingFallback}
		dateRange={{
			startTimestamp: data.dateRange.startTimestamp,
			endTimestamp: data.dateRange.endTimestamp
		}}
		showActionButtons={true}
		showCluster
		showSort={true}
		context="drift"
	/>
</div>

<Separator fullWidthExtend={true} wide={true} />
<CollapsibleSection title="Feature Monitoring" open>
	{#snippet collapsibleContent()}
		<ObservabilityNavTabs />

		<div>
			{#each driftSeriesByGroupName as [groupName, values], i (groupName)}
				<CollapsibleSection title={groupName} size="small" open>
					{#snippet collapsibleContent()}
						<div
							class={cn(
								'h-[274px]',
								i === driftSeriesByGroupName.length - 1 && 'mb-[300px]' // Add extra space at bottom of page for tooltip
							)}
						>
							<FeaturesLineChart
								data={values}
								yDomain={driftMetricDomain}
								onpointclick={(
									_e: MouseEvent,
									{ series, data }: { series: SeriesItem; data: unknown }
								) => {
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
								onitemclick={({ series, data }: { series: SeriesItem; data: DateValue }) => {
									selectSeriesPoint({ series, data });
								}}
								{xDomain}
								onbrushend={(detail: { xDomain?: DomainType }) =>
									detail.xDomain && (xDomain = detail.xDomain)}
								tooltip={{ locked: lockedTooltip }}
							/>
						</div>
					{/snippet}
				</CollapsibleSection>
			{:else}
				<div class="mt-6 bg-destructive/10 border border-destructive/50 p-4 rounded font-medium">
					No drift data available
				</div>
			{/each}
		</div>
	{/snippet}
</CollapsibleSection>

<Dialog
	open={selectedSeriesPoint != null}
	onOpenChange={() => {
		if (selectedSeriesPoint) {
			history.back();
		}
	}}
>
	<DialogContent class="max-w-[85vw] h-[95vh] flex flex-col p-0">
		<DialogHeader class="pt-8 px-7 pb-0">
			<span
				class="mb-4 ml-2 text-xl-medium flex items-center gap-2 w-fit"
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
					{selectedSeriesPoint?.series.key} at {formatDate(selectedSeriesPoint?.data.date)}
				</div>
			</span>

			<ChartControls
				{isZoomed}
				onResetZoom={resetZoom}
				isUsingFallbackDates={data.dateRange.isUsingFallback}
				dateRange={{
					startTimestamp: data.dateRange.startTimestamp,
					endTimestamp: data.dateRange.endTimestamp
				}}
				showActionButtons={false}
				showCluster
				showSort={false}
				context="drift"
			/>
		</DialogHeader>

		<div class="flex-grow px-7 overflow-y-auto">
			{#if selectedSeriesPoint}
				{@const [groupName, values] =
					driftSeriesByGroupName.find(([_, values]) =>
						values.some((value) => value.key?.column === selectedSeriesPoint?.series.key)
					) ?? []}

				{#if groupName && values}
					<CollapsibleSection title={groupName} open={true}>
						{#snippet collapsibleContent()}
							<div class="h-[274px]">
								<FeaturesLineChart
									data={values}
									yDomain={driftMetricDomain}
									markPoint={selectedSeriesPoint?.data}
									onpointclick={(
										_e: MouseEvent,
										{ series, data }: { series: SeriesItem; data: unknown }
									) => {
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
									onitemclick={({ series, data }: { series: SeriesItem; data: DateValue }) => {
										selectSeriesPoint({ series, data });
									}}
									{xDomain}
									onbrushend={(detail: { xDomain?: DomainType }) =>
										detail.xDomain && (xDomain = detail.xDomain)}
									tooltip={{ locked: lockedTooltip }}
								/>
							</div>
						{/snippet}
					</CollapsibleSection>
				{/if}
			{/if}

			{#if columnSummaryData?.percentiles}
				<CollapsibleSection title="Percentiles" open={true}>
					{#snippet collapsibleContent()}
						<div class="h-[230px]">
							<PercentileLineChart
								data={columnSummaryData!}
								{xDomain}
								onbrushend={(detail: { xDomain?: DomainType }) =>
									detail.xDomain && (xDomain = detail.xDomain)}
								tooltip={{ locked: lockedTooltip }}
							/>
						</div>
					{/snippet}
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
				<CollapsibleSection title="Data Distribution" open={true}>
					{#snippet collapsibleContent()}
						<div class="h-[230px]">
							<BarChart
								x="label"
								y="value"
								series={[
									{
										key: 'baseline',
										data: baselineData,
										color: '#4B92FF' // TODO: copied from ECharts defaults
									},
									{
										key: 'current',
										data: currentData,
										color: '#7DFFB3' // TODO: copied from ECharts defaults
									}
								]}
								seriesLayout="group"
								groupPadding={0.1}
								bandPadding={0.1}
								{...barChartProps}
								props={{
									yAxis: { ...yAxisProps },
									xAxis: { ...xAxisProps },
									tooltip: { ...tooltipProps, hideTotal: true }
								}}
							/>
						</div>
					{/snippet}
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

				<CollapsibleSection title="Null Ratio" open={true}>
					{#snippet collapsibleContent()}
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
											cRange={['#4B92FF', '#7DFFB3']}
											{...pieChartProps}
										/>
									</div>
									<div class={cn('text-center', textClass)}>
										{c.label}
									</div>
								</div>
							{/each}
						</div>
					{/snippet}
				</CollapsibleSection>
			{/if}
		</div>
	</DialogContent>
</Dialog>

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

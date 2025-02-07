<script lang="ts">
	import { untrack, type ComponentProps } from 'svelte';
	import { BarChart, PieChart } from 'layerchart';
	import type { DomainType } from 'layerchart/utils/scales';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import type { FeatureResponse } from '$lib/types/Model/Model';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { Api } from '$lib/api/api';
	import { Dialog, DialogContent, DialogHeader } from '$lib/components/ui/dialog';
	import { formatDate } from '$lib/util/format';
	import { METRIC_SCALES } from '$lib/types/MetricType/MetricType';
	import ChartControls from '$lib/components/ChartControls.svelte';
	import type { JoinData } from '$routes/joins/[slug]/services/joins.service';
	import ModelTable from '$routes/joins/[slug]/observability/ModelTable.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import ObservabilityNavTabs from '../ObservabilityNavTabs.svelte';
	import FeaturesLineChart from '$lib/components/charts/FeaturesLineChart.svelte';
	import PercentileLineChart from '$lib/components/charts/PercentileLineChart.svelte';
	import {
		type DateValue,
		barChartProps,
		pieChartProps,
		tooltipProps
	} from '$lib/components/charts/common';
	import { cn } from '$src/lib/utils';
	import { isMacOS } from '$src/lib/util/browser';

	type FeaturesLineChartProps = ComponentProps<typeof FeaturesLineChart>;

	const api = new Api();

	const { data }: { data: JoinData } = $props();

	const metricTypeDomain = $derived.by(() => {
		const scale = METRIC_SCALES[data.metricType];
		return [scale.min, scale.max];
	});

	let isFeatureMonitoringOpen = $state(true);

	type SeriesItem = NonNullable<FeaturesLineChartProps['series']>[number];
	let selectedSeriesPoint = $state<{ series: SeriesItem; data: DateValue } | null>(null);

	let xDomain = $state<DomainType | undefined>(null);
	let isZoomed = $derived(xDomain != null);
	let lockedTooltip = $state(false);

	function resetZoom() {
		xDomain = null;
	}

	let groupSectionStates: { [key: string]: boolean } = $state(
		untrack(() => Object.fromEntries(data.joinTimeseries.items.map((group) => [group.name, true])))
	);

	let percentileData: FeatureResponse | null = $state(null);
	let comparedFeatureData: FeatureResponse | null = $state(null);
	let nullData: FeatureResponse | null = $state(null);

	async function selectSeriesPoint(seriesPoint: typeof selectedSeriesPoint) {
		selectedSeriesPoint = seriesPoint;

		if (seriesPoint) {
			try {
				const featureName = seriesPoint.series.key.toString();

				// TODO: Add loading state
				const [featureData, nullFeatureData] = await Promise.all([
					api.getFeatureTimeseries({
						joinId: data.joinTimeseries.name,
						featureName,
						startTs: data.dateRange.startTimestamp,
						endTs: data.dateRange.endTimestamp,
						granularity: 'percentile',
						metricType: 'drift',
						metrics: 'value',
						offset: '1D',
						algorithm: 'psi'
					}),
					api.getFeatureTimeseries({
						joinId: data.joinTimeseries.name,
						featureName,
						startTs: data.dateRange.startTimestamp,
						endTs: data.dateRange.endTimestamp,
						metricType: 'drift',
						metrics: 'null',
						offset: '1D',
						algorithm: 'psi',
						granularity: 'percentile'
					})
				]);

				if (featureData.isNumeric) {
					percentileData = featureData;
					comparedFeatureData = null;
				} else {
					percentileData = null;
					comparedFeatureData = featureData;
				}

				nullData = nullFeatureData;
			} catch (error) {
				console.error('Error fetching data:', error);
				percentileData = null;
				comparedFeatureData = null;
				nullData = null;
			}
		}
	}
</script>

{#if data.model}
	<ModelTable model={data.model} />
{/if}

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
<CollapsibleSection title="Feature Monitoring" bind:open={isFeatureMonitoringOpen}>
	{#snippet collapsibleContent()}
		<ObservabilityNavTabs />

		<div>
			{#each data.joinTimeseries.items as group, i (group.name)}
				<CollapsibleSection
					title={group.name}
					size="small"
					bind:open={groupSectionStates[group.name]}
				>
					{#snippet collapsibleContent()}
						<div
							class={cn(
								'h-[274px]',
								i === data.joinTimeseries.items.length - 1 && 'mb-[300px]' // Add extra space at bottom of page for tooltip
							)}
						>
							<FeaturesLineChart
								data={group.items}
								yDomain={metricTypeDomain}
								onpointclick={(e, { series, data }) => {
									selectSeriesPoint({ series: series, data: data as unknown as DateValue });
								}}
								onitemclick={({ series, data }) => {
									selectSeriesPoint({ series: series, data });
								}}
								{xDomain}
								onbrushend={(e) => (xDomain = e.xDomain)}
								tooltip={{ locked: lockedTooltip }}
							/>
						</div>
					{/snippet}
				</CollapsibleSection>
			{/each}
		</div>
	{/snippet}
</CollapsibleSection>

<Dialog open={selectedSeriesPoint != null} onOpenChange={() => (selectedSeriesPoint = null)}>
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

		<ScrollArea class="flex-grow px-7">
			{#if selectedSeriesPoint}
				{@const selectedGroup = data.joinTimeseries.items.find((group) =>
					group.items.some((item) => item.feature === selectedSeriesPoint?.series.key)
				)}
				{#if selectedGroup}
					<CollapsibleSection title={selectedGroup.name} open={true}>
						{#snippet collapsibleContent()}
							<div class="h-[274px]">
								<FeaturesLineChart
									data={selectedGroup.items}
									yDomain={metricTypeDomain}
									markPoint={selectedSeriesPoint?.data}
									onpointclick={(e, { series, data }) => {
										selectSeriesPoint({ series: series, data: data as unknown as DateValue });
									}}
									onitemclick={({ series, data }) => {
										selectSeriesPoint({ series: series, data });
									}}
									{xDomain}
									onbrushend={(e) => (xDomain = e.xDomain)}
									tooltip={{ locked: lockedTooltip }}
								/>
							</div>
						{/snippet}
					</CollapsibleSection>
				{/if}
			{/if}

			{#if percentileData}
				<CollapsibleSection title="Percentiles" open={true}>
					{#snippet collapsibleContent()}
						<div class="h-[230px]">
							<PercentileLineChart
								data={percentileData?.current ?? []}
								{xDomain}
								onbrushend={(e) => (xDomain = e.xDomain)}
								tooltip={{ locked: lockedTooltip }}
							/>
						</div>
					{/snippet}
				</CollapsibleSection>
			{/if}

			{#if comparedFeatureData}
				{@const timestamp = Number(selectedSeriesPoint?.data.date)}
				<CollapsibleSection title="Data Distribution" open={true}>
					{#snippet collapsibleContent()}
						<div class="h-[230px]">
							<BarChart
								x="label"
								y="value"
								series={[
									{
										key: 'baseline',
										data: comparedFeatureData?.baseline?.filter((d) => d.ts === timestamp) ?? [],
										color: '#4B92FF' // TODO: copied from ECharts defaults
									},
									{
										key: 'current',
										data: comparedFeatureData?.current?.filter((d) => d.ts === timestamp) ?? [],
										color: '#7DFFB3' // TODO: copied from ECharts defaults
									}
								]}
								seriesLayout="group"
								groupPadding={0.1}
								bandPadding={0.1}
								{...barChartProps}
								props={{
									yAxis: { tweened: { duration: 200 } },
									tooltip: { ...tooltipProps, hideTotal: true }
								}}
							/>
						</div>
					{/snippet}
				</CollapsibleSection>

				<CollapsibleSection title="Null Ratio" open={true}>
					{#snippet collapsibleContent()}
						{#if nullData}
							{@const currentData = nullData.current?.find((point) => point.ts === timestamp)}
							{@const baselineData = nullData.baseline?.find((point) => point.ts === timestamp)}

							<div class="grid grid-cols-2 gap-3">
								{#each [{ label: 'Baseline', data: baselineData }, { label: 'Current', data: currentData }] as c}
									{#if c.data}
										<!-- TODO: Remove mockNullValue once data is populated -->
										{@const mockNullValue = Math.random() * 100}
										<div class="grid gap-2">
											<div class="h-[230px]">
												<!--  TODO: colors copied from ECharts defaults -->
												<PieChart
													data={[
														{
															label: 'Null Value Percentage',
															// value: c.data.nullValue
															value: mockNullValue
														},
														{
															label: 'Non-null Value Percentage',
															// value: 100 - c.data.nullValue
															value: 100 - mockNullValue
														}
													]}
													key="label"
													value="value"
													cRange={['#4B92FF', '#7DFFB3']}
													{...pieChartProps}
												/>
											</div>
											<div class="text-center text-xs text-surface-content">
												{c.label}
											</div>
										</div>
									{/if}
								{/each}
							</div>
						{/if}
					{/snippet}
				</CollapsibleSection>
			{/if}
		</ScrollArea>
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

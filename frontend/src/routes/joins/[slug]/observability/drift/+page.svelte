<script lang="ts">
	import { untrack } from 'svelte';
	import EChart from '$lib/components/EChart.svelte';
	import { connect } from 'echarts';
	import type { EChartOption, EChartsType, ECElementEvent } from 'echarts';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import type { FeatureResponse, TimeSeriesItem } from '$lib/types/Model/Model';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import ResetZoomButton from '$lib/components/ResetZoomButton.svelte';
	import { Button } from '$lib/components/ui/button';
	import { Api } from '$lib/api/api';
	import InfoTooltip from '$lib/components/InfoTooltip.svelte';
	import { Dialog, DialogContent, DialogHeader } from '$lib/components/ui/dialog';
	import { formatDate, formatValue } from '$lib/util/format';
	import PercentileChart from '$lib/components/PercentileChart.svelte';
	import { createChartOption } from '$lib/util/chart-options.svelte';
	import { METRIC_SCALES } from '$lib/types/MetricType/MetricType';
	import { getSeriesColor } from '$lib/util/chart';
	import { handleChartHighlight } from '$lib/util/chart';
	import ChartControls from '$lib/components/ChartControls.svelte';
	import type { JoinData } from '$routes/joins/[slug]/services/joins.service';
	import ModelTable from '$routes/joins/[slug]/observability/ModelTable.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import ObservabilityNavTabs from '$routes/joins/[slug]/observability/ObservabilityNavTabs.svelte';

	const api = new Api();

	const { data }: { data: JoinData } = $props();
	let scale = $derived(METRIC_SCALES[data.metricType]);
	const joinTimeseries = $derived(data.joinTimeseries);
	let isFeatureMonitoringOpen = $state(true);
	let isSheetOpen = $state(false);
	let selectedEvents = $state<ECElementEvent[]>([]);
	let selectedSeries: string | undefined = $state(undefined);
	let isZoomed = $state(false);
	let currentZoomState = $state({ start: 0, end: 100 });

	let percentileData: FeatureResponse | null = $state(null);

	let isComparedFeatureZoomed = $state(false);

	let distributionCharts: { [key: string]: EChartsType } = $state({});

	let groupByCharts: { [key: string]: EChartsType } = $state({});
	let percentileChart: EChartsType | null = $state(null);
	let comparedFeatureChart: EChartsType | null = $state(null);
	let nullRatioChart: EChartsType | null = $state(null);
	let dialogGroupChart: EChartsType | null = $state(null);

	const allCharts = $derived.by(() => {
		const charts: EChartsType[] = [];
		if (dialogGroupChart) charts.push(dialogGroupChart);
		if (percentileChart) charts.push(percentileChart);
		charts.push(...Object.values(groupByCharts));
		charts.push(...Object.values(distributionCharts));
		return charts.filter((chart): chart is EChartsType => chart !== null);
	});

	function createGroupByChartOption(
		features: { feature: string; points: TimeSeriesItem[] }[]
	): EChartOption {
		const series = features.map(({ feature, points }) => ({
			name: feature,
			type: 'line',
			data: points.map((point) => [point.ts, point.value]),
			symbolSize: 16,
			emphasis: {
				focus: 'series',
				itemStyle: {
					borderWidth: 2,
					borderColor: '#fff'
				}
			}
		})) as EChartOption.Series[];

		return createChartOption(
			{
				yAxis: {
					min: scale.min,
					max: scale.max
				},
				series: series as EChartOption.Series[]
			},
			true
		);
	}

	let groupSectionStates: { [key: string]: boolean } = $state(
		untrack(() => Object.fromEntries(joinTimeseries.items.map((group) => [group.name, true])))
	);

	$effect(() => {
		connectCharts();
	});

	function connectCharts() {
		if (allCharts.length > 0) {
			connect(allCharts);
		}
	}

	function handleChartClick(
		event: ECElementEvent,
		chartInstance: EChartsType,
		fromTooltip = false
	) {
		if (event.data) {
			const [timestamp, clickedValue] = event.data as [string, number];
			const date = new Date(timestamp);

			// Get all series data
			const allSeries = chartInstance.getOption().series as EChartOption.Series[];

			// Filter events with matching points
			const matchingEvents = allSeries.flatMap((series) => {
				if (Array.isArray(series.data)) {
					const matchingPointIndex = series.data.findIndex((item) => {
						const [itemTimestamp, itemValue] = item as [string, number];
						return (
							new Date(itemTimestamp).getTime() === date.getTime() && itemValue === clickedValue
						);
					});

					if (matchingPointIndex !== -1) {
						return [
							{
								...event,
								seriesName: series.name
							}
						];
					}
				}
				return [];
			});

			selectedEvents = fromTooltip ? [event] : matchingEvents;
			selectSeries(undefined);

			if (selectedEvents.length === 1) {
				selectSeries(selectedEvents[0].seriesName);
			}
			isSheetOpen = true;
		}
	}

	function resetZoom() {
		allCharts.forEach((chart) => {
			chart.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
		});
		currentZoomState = { start: 0, end: 100 };
		isZoomed = false;
	}

	function handleZoom(event: CustomEvent<EChartOption.DataZoom>) {
		const detail = event.detail as {
			start?: number;
			end?: number;
			batch?: Array<{ startValue: number; endValue: number }>;
		};
		const start = detail.start ?? detail.batch?.[0]?.startValue ?? 0;
		const end = detail.end ?? detail.batch?.[0]?.endValue ?? 100;
		isZoomed = start !== 0 || end !== 100;
		currentZoomState = { start, end };
	}

	function formatEventDate(): string {
		if (!(selectedEvents.length > 0 && selectedEvents[0]?.data !== undefined)) {
			return 'N/A';
		}
		const timestamp = (selectedEvents[0].data as [number, number])[0];
		return formatDate(timestamp);
	}

	function formatEventValue(event: ECElementEvent): string {
		if (Array.isArray(event.data) && event.data.length > 1 && typeof event.data[1] === 'number') {
			return formatValue(event.data[1]);
		}
		return 'N/A';
	}

	async function selectSeries(seriesName: string | undefined) {
		// Reset all data states
		selectedSeries = seriesName;

		if (seriesName) {
			try {
				const [featureData, nullFeatureData] = await Promise.all([
					api.getFeatureTimeseries({
						joinId: joinTimeseries.name,
						featureName: seriesName,
						startTs: data.dateRange.startTimestamp,
						endTs: data.dateRange.endTimestamp,
						granularity: 'percentile',
						metricType: 'drift',
						metrics: 'value',
						offset: '1D',
						algorithm: 'psi'
					}),
					api.getFeatureTimeseries({
						joinId: joinTimeseries.name,
						featureName: seriesName,
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

	let comparedFeatureData: FeatureResponse | null = $state(null);
	let comparedFeatureChartOption = $state({});

	$effect(() => {
		if (selectedSeries && selectedEvents[0]?.data) {
			const timestamp = (selectedEvents[0].data as [number, number])[0];
			comparedFeatureChartOption = createComparedFeatureChartOption(comparedFeatureData, timestamp);
		} else {
			comparedFeatureChartOption = {};
		}
	});

	function createComparedFeatureChartOption(
		data: FeatureResponse | null,
		timestamp: number
	): EChartOption {
		if (!data?.current || !data?.baseline) return {};

		// Get all points at the selected timestamp
		const currentPoints = data.current.filter((point) => point.ts === timestamp);
		const baselinePoints = data.baseline.filter((point) => point.ts === timestamp);

		// Create pairs of current and baseline points by label
		const labels = [
			...new Set([...currentPoints.map((p) => p.label), ...baselinePoints.map((p) => p.label)])
		];

		return createChartOption({
			xAxis: {
				type: 'category',
				data: labels.filter((label): label is string | number => label !== undefined)
			},
			yAxis: {
				type: 'value'
			},
			series: [
				{
					name: 'Baseline',
					type: 'bar',
					emphasis: {
						focus: 'series'
					},
					data: labels.map((label) => {
						const point = baselinePoints.find((p) => p.label === label);
						return point?.value ?? 0;
					})
				} as EChartOption.Series,
				{
					name: 'Current',
					type: 'bar',
					emphasis: {
						focus: 'series'
					},
					data: labels.map((label) => {
						const point = currentPoints.find((p) => p.label === label);
						return point?.value ?? 0;
					})
				} as EChartOption.Series
			]
		});
	}

	let nullData: FeatureResponse | null = $state(null);
	let nullRatioChartOption = $state({});

	$effect(() => {
		if (selectedSeries && selectedEvents[0]?.data) {
			const timestamp = (selectedEvents[0].data as [number, number])[0];
			nullRatioChartOption = createNullRatioChartOption(nullData, timestamp);
		} else {
			nullRatioChartOption = {};
		}
	});

	function createNullRatioChartOption(
		data: FeatureResponse | null,
		timestamp: number
	): EChartOption {
		if (!data?.current || !data?.baseline) return {};

		// Get points at the selected timestamp
		const currentPoint = data.current.find((point) => point.ts === timestamp);
		const baselinePoint = data.baseline.find((point) => point.ts === timestamp);

		if (!currentPoint || !baselinePoint) return {};

		return createChartOption({
			xAxis: {
				type: 'category',
				data: ['Baseline', 'Current']
			},
			yAxis: {
				type: 'value',
				axisLabel: {
					formatter: '{value}%'
				}
			},
			series: [
				{
					name: 'Null Value Percentage',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: [baselinePoint.nullValue, currentPoint.nullValue]
				} as EChartOption.Series,
				{
					name: 'Non-null Value Percentage',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: [100 - baselinePoint.nullValue, 100 - currentPoint.nullValue]
				} as EChartOption.Series
			]
		});
	}

	$effect(() => {
		if (allCharts.length) {
			untrack(() => {
				if (currentZoomState.start !== 0 && currentZoomState.end !== 100) {
					allCharts.forEach((chart) => {
						chart.dispatchAction({
							type: 'dataZoom',
							batch: [{ startValue: currentZoomState.start, endValue: currentZoomState.end }]
						});
					});
				}
			});
		}
	});

	function handleComparedFeatureZoom(event: CustomEvent<EChartOption.DataZoom>) {
		const detail = event.detail as {
			start?: number;
			end?: number;
			batch?: Array<{ startValue: number; endValue: number }>;
		};
		const start = detail.start ?? detail.batch?.[0]?.startValue ?? 0;
		const end = detail.end ?? detail.batch?.[0]?.endValue ?? 100;
		isComparedFeatureZoomed = start !== 0 || end !== 100;
	}

	function resetComparedFeatureZoom() {
		if (comparedFeatureChart) {
			comparedFeatureChart.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
		}
		isComparedFeatureZoomed = false;
	}

	function highlightSeries(
		seriesName: string,
		chart: EChartsType | null,
		type: 'highlight' | 'downplay'
	) {
		if (chart && seriesName) {
			handleChartHighlight(chart, seriesName, type);
		}
	}

	// update selectedEvents when joinTimeseries changes
	$effect(() => {
		if (joinTimeseries) {
			untrack(() => {
				// Only update selectedEvents if we have a previously selected point
				if (selectedEvents.length > 0 && selectedEvents[0]?.data && dialogGroupChart) {
					const [timestamp] = selectedEvents[0].data as [number, number];
					const seriesName = selectedEvents[0].seriesName;

					// Get the updated series data from the chart
					const series = dialogGroupChart.getOption().series as EChartOption.Series[];
					const updatedSeries = series.find((s) => s.name === seriesName);

					if (updatedSeries && Array.isArray(updatedSeries.data)) {
						// Find the point at the same timestamp
						const updatedPoint = updatedSeries.data.find((point) => {
							const [pointTimestamp] = point as [number, number];
							return pointTimestamp === timestamp;
						}) as [number, number] | undefined;

						if (updatedPoint) {
							selectedEvents = [
								{
									...selectedEvents[0],
									data: updatedPoint
								}
							];
						}
					}
				}
			});
		}
	});
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
			{#each joinTimeseries.items as group (group.name)}
				<CollapsibleSection
					title={group.name}
					size="small"
					bind:open={groupSectionStates[group.name]}
				>
					{#snippet collapsibleContent()}
						<EChart
							option={createGroupByChartOption(group.items)}
							bind:chartInstance={groupByCharts[group.name]}
							on:click={(event) =>
								handleChartClick(
									event.detail.detail,
									groupByCharts[group.name],
									event.detail.fromTooltip
								)}
							on:datazoom={handleZoom}
							enableMousemove={true}
							enableCustomZoom={true}
							enableCustomTooltip={true}
							enableTooltipClick={true}
							showCustomLegend={true}
							legendGroup={group}
						/>
					{/snippet}
				</CollapsibleSection>
			{/each}
		</div>
	{/snippet}
</CollapsibleSection>

<Dialog bind:open={isSheetOpen}>
	<DialogContent class="max-w-[85vw] h-[95vh] flex flex-col p-0">
		<DialogHeader class="pt-8 px-7 pb-0">
			<span
				class="mb-4 ml-2 text-xl-medium flex items-center gap-2 w-fit"
				role="presentation"
				onmouseenter={() => highlightSeries(selectedSeries ?? '', dialogGroupChart, 'highlight')}
				onmouseleave={() => highlightSeries(selectedSeries ?? '', dialogGroupChart, 'downplay')}
			>
				{#if selectedSeries && dialogGroupChart}
					<div
						class="w-3 h-3 rounded-full"
						style:background-color={getSeriesColor(dialogGroupChart, selectedSeries)}
					></div>
				{/if}
				<div>
					{selectedSeries ? `${selectedSeries} at ` : ''}{formatEventDate()}
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
			{#if selectedEvents[0]?.seriesName}
				{@const selectedGroup = joinTimeseries.items.find((group) =>
					group.items.some((item) => item.feature === selectedEvents[0].seriesName)
				)}
				{#if selectedGroup}
					<CollapsibleSection title={selectedGroup.name} open={true}>
						{#snippet collapsibleContent()}
							<EChart
								option={createGroupByChartOption(selectedGroup.items)}
								bind:chartInstance={dialogGroupChart}
								markPoint={selectedEvents[0]}
								on:click={(event) =>
									handleChartClick(
										event.detail.detail,
										groupByCharts[selectedGroup.name],
										event.detail.fromTooltip
									)}
								enableMousemove={true}
								enableCustomZoom={true}
								enableCustomTooltip={true}
								enableTooltipClick={true}
								showCustomLegend={true}
								legendGroup={selectedGroup}
							/>
						{/snippet}
					</CollapsibleSection>
				{/if}
			{/if}

			{#if selectedSeries}
				{#if percentileData}
					<CollapsibleSection title="Percentiles" open={true}>
						{#snippet collapsibleContent()}
							<PercentileChart
								data={percentileData?.current ?? null}
								bind:chartInstance={percentileChart}
							/>
						{/snippet}
					</CollapsibleSection>
				{/if}

				{#if comparedFeatureData}
					<CollapsibleSection title="Data Distribution" open={true}>
						{#snippet headerContentRight()}
							{#if isComparedFeatureZoomed}
								<div class="flex justify-end h-6">
									<ResetZoomButton onClick={resetComparedFeatureZoom} />
								</div>
							{/if}
						{/snippet}
						{#snippet collapsibleContent()}
							<EChart
								option={comparedFeatureChartOption}
								bind:chartInstance={comparedFeatureChart}
								enableMousemove={false}
								enableCustomZoom={true}
								on:datazoom={handleComparedFeatureZoom}
								enableCustomTooltip={true}
							/>
						{/snippet}
					</CollapsibleSection>
				{/if}
				<CollapsibleSection title="Null Ratio" open={true}>
					{#snippet collapsibleContent()}
						<EChart
							option={nullRatioChartOption}
							bind:chartInstance={nullRatioChart}
							enableMousemove={false}
							enableCustomZoom={false}
							enableCustomTooltip={true}
						/>
					{/snippet}
				</CollapsibleSection>
			{:else if selectedEvents.length > 1}
				<div class="max-w-md p-2">
					<div class="text-large-medium mb-4 flex items-center gap-2">
						Select a data point
						<InfoTooltip
							text="Multiple data points overlap at this timestamp. Select one to view its details."
						/>
					</div>
					{#each selectedEvents as event}
						<Button
							variant="ghost"
							class="p-2 [&:not(:last-child)]:border-b w-full"
							on:click={() => selectSeries(event.seriesName)}
							autofocus={true}
							onmouseenter={() =>
								highlightSeries(event.seriesName ?? '', dialogGroupChart, 'highlight')}
							onmouseleave={() =>
								highlightSeries(event.seriesName ?? '', dialogGroupChart, 'downplay')}
						>
							<span class="flex justify-between items-center w-full">
								<span class="text-neutral-800 truncate flex items-center gap-2">
									<div
										class="w-3 h-3 rounded-full"
										style:background-color={getSeriesColor(
											dialogGroupChart,
											event.seriesName ?? ''
										)}
									></div>
									{event.seriesName ?? ''}
								</span>
								<span>{formatEventValue(event)}</span>
							</span>
						</Button>
					{/each}
				</div>
			{/if}
		</ScrollArea>
	</DialogContent>
</Dialog>

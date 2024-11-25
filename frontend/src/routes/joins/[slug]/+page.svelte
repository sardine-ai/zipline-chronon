<script lang="ts">
	import EChart from '$lib/components/EChart/EChart.svelte';
	import type { EChartOption, EChartsType, ECElementEvent } from 'echarts';
	import merge from 'lodash/merge';

	import { Tabs, TabsList, TabsTrigger, TabsContent } from '$lib/components/ui/tabs';
	import { Icon, TableCells } from 'svelte-hero-icons';
	import { ChartLine } from '@zipline-ai/icons';
	import CollapsibleSection from '$lib/components/CollapsibleSection/CollapsibleSection.svelte';
	import { connect } from 'echarts';
	import type {
		FeatureResponse,
		NullComparedFeatureResponse,
		RawComparedFeatureResponse,
		TimeSeriesItem
	} from '$lib/types/Model/Model';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { untrack } from 'svelte';
	import PageHeader from '$lib/components/PageHeader/PageHeader.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import DriftSkewToggle from '$lib/components/DriftSkewToggle/DriftSkewToggle.svelte';
	import ResetZoomButton from '$lib/components/ResetZoomButton/ResetZoomButton.svelte';
	import DateRangeSelector from '$lib/components/DateRangeSelector/DateRangeSelector.svelte';
	import IntersectionObserver from 'svelte-intersection-observer';
	import { fade } from 'svelte/transition';
	import { Button } from '$lib/components/ui/button';
	import { parseDateRangeParams } from '$lib/util/date-ranges';
	import { getFeatureTimeseries } from '$lib/api/api';
	import { page } from '$app/stores';
	import { comparedFeatureNumericalSampleData, nullCountSampleData } from '$lib/util/sample-data';
	import { getCssColorAsHex } from '$lib/util/colors.js';
	import InfoTooltip from '$lib/components/InfoTooltip/InfoTooltip.svelte';
	import { Table, TableBody, TableCell, TableRow } from '$lib/components/ui/table/index.js';
	import TrueFalseBadge from '$lib/components/TrueFalseBadge/TrueFalseBadge.svelte';
	import CustomEChartLegend from '$lib/components/CustomEChartLegend/CustomEChartLegend.svelte';
	import ActionButtons from '$lib/components/ActionButtons/ActionButtons.svelte';
	import { Dialog, DialogContent, DialogHeader } from '$lib/components/ui/dialog';
	import { formatDate, formatValue } from '$lib/util/format';

	const { data } = $props();
	let selectedDriftSkew = $state<'drift' | 'skew'>('drift');
	const joinTimeseries = $derived(data.joinTimeseries);
	const timeseries = $derived(data.timeseries);
	const model = $derived(data.model);
	let isModelPerformanceOpen = $state(true);
	let isModelDriftOpen = $state(true);
	let isFeatureMonitoringOpen = $state(true);
	let isSheetOpen = $state(false);
	let selectedEvents = $state<ECElementEvent[]>([]);
	let selectedSeries: string | undefined = $state(undefined);
	let isZoomed = $state(false);
	let currentZoomState = $state({ start: 0, end: 100 });
	const chartData = $derived(timeseries.items.map((item) => [item.ts, item.value]));

	let driftSkewIntersectionElement: HTMLElement | null = $state(null);
	let dateRangeSelectorIntersectionElement: HTMLElement | null = $state(null);
	let isDriftSkewVisible = $state(true);
	let isDateRangeSelectorVisible = $state(true);

	let percentileData: FeatureResponse | null = $state(null);

	let isComparedFeatureZoomed = $state(false);

	function createChartOption(
		customOption: Partial<EChartOption> = {},
		customColors = false
	): EChartOption {
		const defaultOption: EChartOption = {
			color: customColors
				? [
						'#E5174B',
						'#E54D4A',
						'#E17545',
						'#E3994C',
						'#DFAF4F',
						'#87BE52',
						'#53B167',
						'#4DA67D',
						'#4EA797',
						'#4491CE',
						'#4592CC',
						'#4172D2',
						'#5B5AD1',
						'#785AD4',
						'#9055D5',
						'#BF50D3',
						'#CB5587'
					]
				: undefined,
			tooltip: {
				trigger: 'axis',
				axisPointer: {
					type: 'line',
					lineStyle: {
						color: neutral700,
						type: 'solid'
					}
				},
				position: 'top',
				confine: true
			},
			xAxis: {
				type: 'time',
				axisLabel: {
					formatter: {
						month: '{MMM} {d}',
						day: '{MMM} {d}'
					} as unknown as string,
					color: neutral700
				},
				splitLine: {
					show: true,
					lineStyle: {
						color: neutral300
					}
				},
				axisLine: {
					lineStyle: {
						color: neutral300
					}
				}
			},
			yAxis: {
				type: 'value',
				axisLabel: {
					formatter: (value: number) => (value % 1 === 0 ? value.toFixed(0) : value.toFixed(1)),
					color: neutral700
				},
				splitLine: {
					show: true,
					lineStyle: {
						color: neutral300
					}
				},
				axisLine: {
					lineStyle: {
						color: neutral300
					}
				}
			},
			grid: {
				top: 5,
				right: 1,
				bottom: 0,
				left: 0,
				containLabel: true
			}
		};

		const baseSeriesStyle = {
			showSymbol: false,
			lineStyle: {
				width: 1
			},
			symbolSize: 7
		};

		if (customOption.series) {
			const series = Array.isArray(customOption.series)
				? customOption.series
				: [customOption.series];
			customOption.series = series.map((s) => merge({}, baseSeriesStyle, s));
		}

		return merge({}, defaultOption, customOption);
	}

	let neutral300 = $state('');
	let neutral700 = $state('');

	$effect(() => {
		neutral300 = getCssColorAsHex('--neutral-300');
		neutral700 = getCssColorAsHex('--neutral-700');
	});

	let groupByCharts: { [key: string]: EChartsType } = $state({});
	let performanceChart: EChartsType | null = $state(null);
	let percentileChart: EChartsType | null = $state(null);
	let comparedFeatureChart: EChartsType | null = $state(null);
	let nullRatioChart: EChartsType | null = $state(null);
	let driftChart: EChartsType | null = $state(null);
	let dialogGroupChart: EChartsType | null = $state(null);
	const performanceChartOption = $derived(
		createChartOption(
			{
				yAxis: {
					min: 0,
					max: 1,
					interval: 0.2
				},
				series: [
					{
						data: chartData,
						type: 'line'
					}
				]
			},
			true
		)
	);
	const driftChartOption = $derived(
		createChartOption(
			{
				yAxis: {
					min: 0,
					max: 1,
					interval: 0.2
				},
				series: [
					{
						data: chartData,
						type: 'line'
					}
				]
			},
			true
		)
	);

	const allCharts = $derived.by(() => {
		const charts: EChartsType[] = [];
		if (performanceChart) charts.push(performanceChart);
		if (driftChart) charts.push(driftChart);
		if (dialogGroupChart) charts.push(dialogGroupChart);
		if (percentileChart) charts.push(percentileChart);
		charts.push(...Object.values(groupByCharts));
		return charts.filter((chart): chart is EChartsType => chart !== null);
	});

	function createGroupByChartOption(
		features: { feature: string; points: TimeSeriesItem[] }[]
	): EChartOption {
		const series = features.map(({ feature, points }) => ({
			name: feature,
			type: 'line',
			data: points.map((point) => [point.ts, point.value]),
			emphasis: {
				focus: 'series'
			}
		})) as EChartOption.Series[];

		return createChartOption(
			{
				yAxis: {
					min: 0,
					max: 1,
					interval: 0.2
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

	$effect(() => {
		if (timeseries) {
			untrack(() => {
				resetZoom();
			});
		}
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

	const shouldShowStickyHeader = $derived(!isDriftSkewVisible && !isDateRangeSelectorVisible);

	function formatEventDate(): string {
		if (!(selectedEvents.length > 0 && selectedEvents[0]?.data !== undefined)) {
			return 'N/A';
		}
		const timestamp = (selectedEvents[0].data as [number, number])[0];
		return formatDate(new Date(timestamp).toLocaleString());
	}

	function formatEventValue(event: ECElementEvent): string {
		if (Array.isArray(event.data) && event.data.length > 1 && typeof event.data[1] === 'number') {
			return formatValue(event.data[1]);
		}
		return 'N/A';
	}

	async function selectSeries(seriesName: string | undefined) {
		selectedSeries = seriesName;
		if (seriesName) {
			const { startTimestamp, endTimestamp } = parseDateRangeParams(
				new URL($page.url).searchParams
			);
			try {
				percentileData = await getFeatureTimeseries(seriesName, startTimestamp, endTimestamp);
			} catch (error) {
				console.error('Error fetching percentile data:', error);
				percentileData = null;
			}
		}
	}

	function getPercentileChartSeries(data: FeatureResponse): EChartOption.Series[] {
		const targetLabels = ['p5', 'p50', 'p95'];
		return targetLabels
			.filter((label) => data.points.some((point) => point.label === label))
			.map((label) => ({
				name: label,
				type: 'line',
				stack: 'Total',
				emphasis: {
					focus: 'series'
				},
				data: data.points
					.filter((point) => point.label === label)
					.map((point) => [point.ts, point.value])
			})) as EChartOption.Series[];
	}

	function getPercentileChartOption(data: FeatureResponse): EChartOption {
		const processedData = getPercentileChartSeries(data);
		return createChartOption({
			xAxis: {
				type: 'time',
				splitLine: {
					show: true
				}
			},
			yAxis: {
				type: 'value',

				splitLine: {
					show: true
				}
			},
			series: processedData
		});
	}

	const percentileChartOption = $derived.by(() => {
		if (!percentileData) return {};
		return getPercentileChartOption(percentileData);
	});

	$effect(() => {
		selectSeries(selectedSeries);
	});

	function createComparedFeatureChartOption(data: RawComparedFeatureResponse): EChartOption {
		return createChartOption({
			xAxis: {
				type: 'category',
				data: data.x
			},
			yAxis: {
				type: 'value'
			},
			series: [
				{
					name: 'Baseline',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: data.old
				} as EChartOption.Series,
				{
					name: 'Current',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: data.new
				} as EChartOption.Series
			]
		});
	}

	function createNullRatioChartOption(data: NullComparedFeatureResponse): EChartOption {
		return createChartOption({
			xAxis: {
				type: 'category',
				data: ['Null Values', 'Non-null Values']
			},
			yAxis: {
				type: 'value'
			},
			series: [
				{
					name: 'Baseline',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: [data.oldNullCount, data.oldValueCount]
				} as EChartOption.Series,
				{
					name: 'Current',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: [data.newNullCount, data.newValueCount]
				} as EChartOption.Series
			]
		});
	}

	let comparedFeatureChartOption = $state({});
	$effect(() => {
		comparedFeatureChartOption = createComparedFeatureChartOption(
			comparedFeatureNumericalSampleData
		);
	});

	let nullRatioChartOption = $state({});
	$effect(() => {
		nullRatioChartOption = createNullRatioChartOption(nullCountSampleData);
	});

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

	function getSeriesColor(seriesName: string, chart: EChartsType | null): string | undefined {
		if (!chart) return undefined;

		const options = chart.getOption();
		if (!options || !options.color || !options.series) return undefined;

		const seriesIndex = options.series.findIndex((s) => s.name === seriesName);
		if (seriesIndex === -1) return undefined;

		return options.color[seriesIndex];
	}
</script>

{#if shouldShowStickyHeader}
	<div
		class="sticky top-0 z-20 bg-neutral-200 border-b border-border -mx-8 py-2 px-8 border-l"
		transition:fade={{ duration: 150 }}
	>
		<div class="flex items-center justify-end space-x-6">
			{#if isZoomed}
				<ResetZoomButton onClick={resetZoom} />
			{/if}
			<DriftSkewToggle bind:selected={selectedDriftSkew} />
			<DateRangeSelector />
		</div>
	</div>
{/if}

<PageHeader title={timeseries.id}></PageHeader>

{#if model}
	<div class="border rounded-md w-1/2 mb-6">
		<Table density="compact">
			<TableBody>
				<TableRow>
					<TableCell align="left">Team</TableCell>
					<TableCell align="right">{model.team}</TableCell>
				</TableRow>
				<TableRow>
					<TableCell align="left">Model Type</TableCell>
					<TableCell align="right">{model.modelType}</TableCell>
				</TableRow>
				<TableRow>
					<TableCell align="left">Production</TableCell>
					<TableCell align="right">
						<TrueFalseBadge isTrue={model.production} />
					</TableCell>
				</TableRow>
			</TableBody>
		</Table>
	</div>
{/if}

<Separator fullWidthExtend={true} wide={true} />

<CollapsibleSection title="Model Performance" bind:open={isModelPerformanceOpen}>
	{#snippet headerContentLeft()}
		<InfoTooltip text="Model performance compares predictions to labels or ground truth." />
	{/snippet}
	{#snippet headerContentRight()}
		<div class="flex items-center justify-between">
			{#if isZoomed}
				<ResetZoomButton onClick={resetZoom} class="mr-4" />
			{/if}
			<IntersectionObserver
				element={dateRangeSelectorIntersectionElement}
				bind:intersecting={isDateRangeSelectorVisible}
			>
				<div bind:this={dateRangeSelectorIntersectionElement}></div>
			</IntersectionObserver>
			<DateRangeSelector />
		</div>
	{/snippet}
	{#snippet collapsibleContent()}
		<EChart
			option={performanceChartOption}
			bind:chartInstance={performanceChart}
			on:datazoom={handleZoom}
			enableMousemove={false}
			enableCustomZoom={true}
			enableCustomTooltip={true}
		/>
	{/snippet}
</CollapsibleSection>

<Separator fullWidthExtend={true} wide={true} />
<CollapsibleSection title="Model Drift trends" bind:open={isModelDriftOpen}>
	{#snippet headerContentLeft()}
		<InfoTooltip
			text="Model drift occurs when a model's accuracy decreases over time as the data it was trained on differs from new data."
		/>
	{/snippet}
	{#snippet headerContentRight()}
		<IntersectionObserver
			element={driftSkewIntersectionElement}
			bind:intersecting={isDriftSkewVisible}
		>
			<div bind:this={driftSkewIntersectionElement}></div>
		</IntersectionObserver>
		<DriftSkewToggle bind:selected={selectedDriftSkew} />
	{/snippet}
	{#snippet collapsibleContent()}
		<EChart
			option={driftChartOption}
			bind:chartInstance={driftChart}
			on:datazoom={handleZoom}
			enableMousemove={false}
			enableCustomZoom={true}
			enableCustomTooltip={true}
		/>
	{/snippet}
</CollapsibleSection>

<Separator fullWidthExtend={true} wide={true} />
<CollapsibleSection title="Feature Monitoring" bind:open={isFeatureMonitoringOpen}>
	{#snippet collapsibleContent()}
		<Tabs value="monitoring" class="w-full">
			<TabsList>
				<TabsTrigger value="features" class="flex items-center">
					<Icon src={TableCells} micro size="16" class="mr-2" />
					Features
				</TabsTrigger>
				<TabsTrigger value="monitoring" class="flex items-center">
					<Icon src={ChartLine} size="16" class="mr-2" />
					Monitoring
				</TabsTrigger>
			</TabsList>
			<Separator fullWidthExtend={true} />

			<TabsContent value="features">
				<div class="p-4">
					<h3 class="text-lg font-semibold">Features Content</h3>
				</div>
			</TabsContent>
			<TabsContent value="monitoring">
				<ActionButtons showCluster={true} class="mt-8" />

				{#each joinTimeseries.items as group}
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
							/>
							<CustomEChartLegend
								groupName={group.name}
								items={group.items}
								chart={groupByCharts[group.name]}
							/>
						{/snippet}
					</CollapsibleSection>
				{/each}
			</TabsContent>
		</Tabs>
	{/snippet}
</CollapsibleSection>

<Dialog bind:open={isSheetOpen}>
	<DialogContent class="max-w-[85vw] h-[95vh] flex flex-col p-0">
		<DialogHeader class="pt-8 px-7 pb-0">
			<span class="ml-2 text-xl-medium flex items-center gap-2">
				{#if selectedSeries && dialogGroupChart}
					<div
						class="w-3 h-3 rounded-full"
						style:background-color={getSeriesColor(selectedSeries, dialogGroupChart)}
					></div>
				{/if}
				<div>
					{selectedSeries && selectedSeries + ' at'}
				</div>
				{formatEventDate()}
			</span>
		</DialogHeader>

		<ScrollArea class="flex-grow px-7">
			{#if selectedEvents[0]?.seriesName}
				{@const selectedGroup = joinTimeseries.items.find((group) =>
					group.items.some((item) => item.feature === selectedEvents[0].seriesName)
				)}
				{#if selectedGroup}
					<CollapsibleSection title={selectedGroup.name} open={true}>
						{#snippet headerContentRight()}
							<div class="flex items-center justify-between space-x-6">
								<div class="flex justify-end">
									{#if isZoomed}
										<ResetZoomButton onClick={resetZoom} />
									{/if}
								</div>
								<DateRangeSelector />
							</div>
						{/snippet}
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
							/>
							{#if dialogGroupChart}
								<CustomEChartLegend
									groupName={selectedGroup.name}
									items={selectedGroup.items}
									chart={dialogGroupChart}
								/>
							{/if}
						{/snippet}
					</CollapsibleSection>
				{/if}
			{/if}

			{#if selectedSeries}
				<CollapsibleSection title="Percentiles" open={true}>
					{#snippet collapsibleContent()}
						<EChart
							option={percentileChartOption}
							bind:chartInstance={percentileChart}
							enableMousemove={false}
							enableCustomZoom={true}
							enableCustomTooltip={true}
						/>
					{/snippet}
				</CollapsibleSection>

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
						>
							<span class="flex justify-between items-center w-full">
								<span class="text-neutral-800 truncate flex items-center gap-2">
									<div
										class="w-3 h-3 rounded-full"
										style:background-color={getSeriesColor(
											event.seriesName ?? '',
											dialogGroupChart
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

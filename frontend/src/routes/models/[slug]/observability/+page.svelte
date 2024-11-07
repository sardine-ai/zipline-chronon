<script lang="ts">
	import EChart from '$lib/components/EChart/EChart.svelte';
	import type { EChartOption, EChartsType, ECElementEvent } from 'echarts';
	import merge from 'lodash/merge';

	import { Tabs, TabsList, TabsTrigger, TabsContent } from '$lib/components/ui/tabs';
	import { Icon, TableCells, ChartBar, ChevronLeft } from 'svelte-hero-icons';
	import CollapsibleSection from '$lib/components/CollapsibleSection/CollapsibleSection.svelte';
	import { connect } from 'echarts';
	import type {
		FeatureResponse,
		RawComparedFeatureResponse,
		TimeSeriesItem
	} from '$lib/types/Model/Model';
	import { Sheet, SheetContent, SheetHeader } from '$lib/components/ui/sheet';
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
	import { comparedFeatureNumericalSampleData } from '$lib/util/sample-data';
	import { comparedFeatureCategoricalSampleData } from '$lib/util/sample-data';

	const { data } = $props();
	let selectedDriftSkew = $state<'drift' | 'skew'>('drift');
	const joinTimeseries = $derived(data.joinTimeseries);
	const timeseries = $derived(data.timeseries);
	let isModelPerformanceOpen = $state(true);
	let isModelDriftOpen = $state(true);
	let isFeatureMonitoringOpen = $state(true);
	let isSheetOpen = $state(false);
	let selectedEvents = $state<ECElementEvent[]>([]);
	let selectedSeries: string | undefined = $state(undefined);
	let isZoomed = $state(false);
	const chartData = $derived(timeseries.items.map((item) => [item.ts, item.value]));

	let driftSkewIntersectionElement: HTMLElement | null = $state(null);
	let dateRangeSelectorIntersectionElement: HTMLElement | null = $state(null);
	let isDriftSkewVisible = $state(true);
	let isDateRangeSelectorVisible = $state(true);

	let percentileData: FeatureResponse | null = $state(null);

	function createChartOption(customOption: Partial<EChartOption> = {}): EChartOption {
		const defaultOption: EChartOption = {
			xAxis: {
				type: 'time',
				axisLabel: {
					formatter: {
						month: '{MMM} {d}',
						day: '{MMM} {d}'
					} as unknown as string
				},
				splitLine: {
					show: true
				}
			},
			yAxis: {
				type: 'value',
				axisLabel: {
					formatter: (value: number) => (value % 1 === 0 ? value.toFixed(0) : value.toFixed(1))
				},
				splitLine: {
					show: true
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

		return merge({}, defaultOption, customOption);
	}

	function createTooltip(title: string): EChartOption.Tooltip {
		return {
			trigger: 'axis',
			formatter: function (params: EChartOption.Tooltip.Format | EChartOption.Tooltip.Format[]) {
				if (Array.isArray(params) && params.length > 0 && Array.isArray(params[0].value)) {
					const date = new Date(params[0].value[0]);
					const value = params[0].value[1];
					return `${date.toLocaleString()}<br/>${title}: ${typeof value === 'number' ? value.toFixed(4) : 'N/A'}`;
				}
				return 'Invalid data';
			}
		};
	}

	let groupByCharts: { [key: string]: EChartsType } = $state({});
	let performanceChart: EChartsType | null = $state(null);
	let percentileChart: EChartsType | null = $state(null);
	let comparedFeatureNumericalChart: EChartsType | null = $state(null);
	let comparedFeatureCategoricalChart: EChartsType | null = $state(null);
	let driftChart: EChartsType | null = $state(null);
	const performanceChartOption = $derived(
		createChartOption({
			yAxis: {
				min: 0,
				max: 1,
				interval: 0.2
			},
			tooltip: createTooltip('Performance'),
			series: [
				{
					data: chartData,
					type: 'line'
				}
			]
		})
	);
	const driftChartOption = $derived(
		createChartOption({
			yAxis: {
				min: 0,
				max: 1,
				interval: 0.2
			},
			tooltip: createTooltip('Drift'),
			series: [
				{
					data: chartData,
					type: 'line'
				}
			]
		})
	);

	const allCharts = $derived.by(() => {
		const charts: EChartsType[] = [];
		if (performanceChart) charts.push(performanceChart);
		if (driftChart) charts.push(driftChart);
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
				focus: 'self',
				scale: 2
			},
			symbolSize: 5
		}));

		return createChartOption({
			yAxis: {
				min: 0,
				max: 1,
				interval: 0.2
			},
			series: series as EChartOption.Series[]
		});
	}

	let groupSectionStates: { [key: string]: boolean } = $derived(
		Object.fromEntries(joinTimeseries.items.map((group) => [group.name, true]))
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

	function handleChartClick(event: ECElementEvent, chartInstance: EChartsType) {
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

			selectedEvents = matchingEvents;
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
		isZoomed = false;
	}

	function handleZoom(event: EChartOption.DataZoom) {
		isZoomed = event.start !== 0 || event.end !== 100;
	}

	const shouldShowStickyHeader = $derived(!isDriftSkewVisible && !isDateRangeSelectorVisible);

	function formatEventDate(): string {
		if (!(selectedEvents.length > 0 && selectedEvents[0]?.data !== undefined)) {
			return 'N/A';
		}
		const timestamp = (selectedEvents[0].data as [number, number])[0];
		return new Date(timestamp).toLocaleString();
	}

	function formatEventValue(event: ECElementEvent): string {
		if (Array.isArray(event.data) && event.data.length > 1 && typeof event.data[1] === 'number') {
			return event.data[1].toFixed(4);
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
			tooltip: {
				trigger: 'axis',
				axisPointer: {
					type: 'cross'
				},
				position: 'top',
				confine: true
			},
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

	function createComparedFeatureNumericalChartOption(
		data: RawComparedFeatureResponse
	): EChartOption {
		return createChartOption({
			tooltip: {
				trigger: 'axis',
				axisPointer: {
					type: 'cross'
				}
			},
			xAxis: {
				type: 'value'
			},
			yAxis: {
				type: 'value'
			},
			series: [
				{
					name: 'Baseline',
					type: 'line',
					areaStyle: {},
					emphasis: {
						focus: 'series'
					},
					data: data.baseline.map((point) => [point.label, point.value])
				} as EChartOption.Series,
				{
					name: 'Current',
					type: 'line',
					areaStyle: {},
					emphasis: {
						focus: 'series'
					},
					data: data.current.map((point) => [point.label, point.value])
				} as EChartOption.Series
			]
		});
	}

	let comparedFeatureNumericalChartOption = $state({});
	$effect(() => {
		comparedFeatureNumericalChartOption = createComparedFeatureNumericalChartOption(
			comparedFeatureNumericalSampleData
		);
	});

	function createComparedFeatureCategoricalChartOption(
		data: RawComparedFeatureResponse
	): EChartOption {
		const categories = Array.from(
			new Set([...data.baseline, ...data.current].map((item) => item.label))
		).filter((cat): cat is string => cat !== null);

		return createChartOption({
			tooltip: {
				trigger: 'axis',
				axisPointer: {
					type: 'shadow'
				}
			},
			legend: {
				data: ['Baseline', 'Current']
			},
			xAxis: {
				type: 'category',
				data: categories
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
					data: categories.map(
						(cat) => data.baseline.find((item) => item.label === cat)?.value || 0
					)
				} as EChartOption.Series,
				{
					name: 'Current',
					type: 'bar',
					stack: 'total',
					emphasis: {
						focus: 'series'
					},
					data: categories.map((cat) => data.current.find((item) => item.label === cat)?.value || 0)
				} as EChartOption.Series
			]
		});
	}

	let comparedFeatureCategoricalChartOption = $state({});
	$effect(() => {
		comparedFeatureCategoricalChartOption = createComparedFeatureCategoricalChartOption(
			comparedFeatureCategoricalSampleData
		);
	});
</script>

{#if shouldShowStickyHeader}
	<div
		class="sticky top-0 z-10 bg-background border-b border-border -mx-8 py-2 px-8"
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

<PageHeader title="Model {timeseries.id}"></PageHeader>
<Separator fullWidthExtend={true} wide={true} />

<CollapsibleSection title="Model Performance" bind:open={isModelPerformanceOpen}>
	{#snippet headerContentRight()}
		<div class="flex items-center justify-between">
			{#if isZoomed}
				<ResetZoomButton onClick={resetZoom} />
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
	{#snippet collabsibleContent()}
		<EChart
			option={performanceChartOption}
			bind:chartInstance={performanceChart}
			on:datazoom={handleZoom}
			enableMousemove={false}
			enableCustomZoom={true}
			height="200px"
		/>
	{/snippet}
</CollapsibleSection>

<Separator fullWidthExtend={true} wide={true} />
<CollapsibleSection title="Model Drift" bind:open={isModelDriftOpen}>
	{#snippet headerContentRight()}
		<IntersectionObserver
			element={driftSkewIntersectionElement}
			bind:intersecting={isDriftSkewVisible}
		>
			<div bind:this={driftSkewIntersectionElement}></div>
		</IntersectionObserver>
		<DriftSkewToggle bind:selected={selectedDriftSkew} />
	{/snippet}
	{#snippet collabsibleContent()}
		<EChart
			option={driftChartOption}
			bind:chartInstance={driftChart}
			on:datazoom={handleZoom}
			enableMousemove={false}
			enableCustomZoom={true}
			height="200px"
		/>
	{/snippet}
</CollapsibleSection>

<Separator fullWidthExtend={true} wide={true} />
<CollapsibleSection title="Feature Monitoring" bind:open={isFeatureMonitoringOpen}>
	{#snippet collabsibleContent()}
		<Tabs value="monitoring" class="w-full">
			<TabsList>
				<TabsTrigger value="features" class="flex items-center">
					<Icon src={TableCells} micro size="16" class="mr-2" />
					Features
				</TabsTrigger>
				<TabsTrigger value="monitoring" class="flex items-center">
					<Icon src={ChartBar} micro size="16" class="mr-2" />
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
				<div class="p-4">
					{#each joinTimeseries.items as group}
						<CollapsibleSection title={group.name} bind:open={groupSectionStates[group.name]}>
							{#snippet collabsibleContent()}
								<EChart
									option={createGroupByChartOption(group.items)}
									bind:chartInstance={groupByCharts[group.name]}
									on:click={(event) => handleChartClick(event.detail, groupByCharts[group.name])}
									on:datazoom={handleZoom}
									enableMousemove={true}
									enableCustomZoom={true}
									height="200px"
								/>
							{/snippet}
						</CollapsibleSection>
					{/each}
				</div>
			</TabsContent>
		</Tabs>
	{/snippet}
</CollapsibleSection>

<Sheet bind:open={isSheetOpen}>
	<SheetContent
		class="flex flex-col h-full !max-w-full {selectedSeries ? 'w-[560px]' : 'w-[352px]'}"
		noAnimation={true}
	>
		<SheetHeader>
			{#if selectedSeries}
				<div class="flex items-center justify-start">
					<Button variant="ghost" on:click={() => selectSeries(undefined)} size="icon">
						<Icon src={ChevronLeft} micro size="16" color="red-500" />
					</Button>
					<span class="ml-2 text-xl">{selectedSeries}</span>
				</div>
			{:else}
				{formatEventDate()}
			{/if}
		</SheetHeader>
		{#if selectedSeries}
			<ScrollArea class="flex-grow">
				<div class="text-md my-3">Percentiles</div>
				<EChart
					option={percentileChartOption}
					bind:chartInstance={percentileChart}
					enableMousemove={false}
					enableCustomZoom={true}
					height="400px"
				/>
				<div class="text-md my-3">Now vs Baseline (Numeric data)</div>
				<EChart
					option={comparedFeatureNumericalChartOption}
					bind:chartInstance={comparedFeatureNumericalChart}
					enableMousemove={false}
					enableCustomZoom={true}
					height="400px"
				/>
				<div class="text-md my-3">Now vs Baseline (Categorical data)</div>
				<EChart
					option={comparedFeatureCategoricalChartOption}
					bind:chartInstance={comparedFeatureCategoricalChart}
					enableMousemove={false}
					height="400px"
				/>
			</ScrollArea>
		{:else if selectedEvents.length > 1}
			<ScrollArea class="flex-grow">
				<div class="border rounded-md">
					{#each selectedEvents as event}
						<Button
							variant="ghost"
							class="p-2 [&:not(:last-child)]:border-b w-full"
							on:click={() => selectSeries(event.seriesName)}
						>
							<span class="flex justify-between items-center w-full">
								<span>{event.seriesName}</span>
								<span>{formatEventValue(event)}</span>
							</span>
						</Button>
					{/each}
				</div>
			</ScrollArea>
		{/if}
	</SheetContent>
</Sheet>

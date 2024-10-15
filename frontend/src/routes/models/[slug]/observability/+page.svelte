<script lang="ts">
	import { DATE_RANGE_PARAM, DATE_RANGES, type DateRangeOption } from '$lib/constants/date-ranges';
	import EChart from '$lib/components/EChart/EChart.svelte';
	import type { EChartOption, EChartsType, ECElementEvent } from 'echarts';
	import merge from 'lodash/merge';
	import {
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue
	} from '$lib/components/ui/select';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import type { Selected } from 'bits-ui';
	import { Tabs, TabsList, TabsTrigger, TabsContent } from '$lib/components/ui/tabs';
	import { Table, BarChart } from 'svelte-radix';
	import CollapsibleSection from '$lib/components/CollapsibleSection/CollapsibleSection.svelte';
	import { connect } from 'echarts';
	import type { TimeSeriesItem } from '$lib/types/Model/Model';
	import {
		Sheet,
		SheetContent,
		SheetDescription,
		SheetHeader,
		SheetTitle
	} from '$lib/components/ui/sheet';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { onMount, untrack } from 'svelte';
	import { Button } from '$lib/components/ui/button';

	const { data } = $props();
	const joinTimeseries = $derived(data.joinTimeseries);
	const timeseries = $derived(data.timeseries);
	let dateRange = $state<DateRangeOption>(data.dateRange);
	let isModelPerformanceOpen = $state(true);
	let isModelDriftOpen = $state(true);
	let isFeatureMonitoringOpen = $state(true);
	let isSheetOpen = $state(false);
	let selectedPoints = $state<
		{
			date: string;
			value: number | string;
			title: string;
			line: string;
		}[]
	>([]);
	let isZoomed = $state(false);
	const chartData = $derived(timeseries.items.map((item) => [item.ts, item.value]));

	function createChartOption(customOption: Partial<EChartOption> = {}): EChartOption {
		const defaultOption: EChartOption = {
			xAxis: {
				type: 'time',
				axisLabel: {
					formatter: (value: string) => new Date(value).toLocaleDateString()
				}
			},
			yAxis: {
				type: 'value',
				min: 0,
				max: 1,
				axisLabel: {
					formatter: (value: number) => value.toFixed(2)
				}
			},
			toolbox: {
				feature: {
					dataZoom: {
						yAxisIndex: 'none',
						icon: {
							zoom: 'path://', // hack to remove zoom button
							back: 'path://' // hack to remove restore button
						}
					}
				}
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
	let driftChart: EChartsType | null = $state(null);
	const performanceChartOption = $derived(
		createChartOption({
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

	function handleDateRangeChange(value: Selected<string> | undefined) {
		if (value) {
			const url = new URL($page.url);
			url.searchParams.set(DATE_RANGE_PARAM, value.value);
			goto(url.toString(), { replaceState: true });
		}
	}

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
				allCharts.forEach(activateZoom);
				resetZoom();
			});
		}
	});

	function connectCharts() {
		if (allCharts.length > 0) {
			connect(allCharts);
		}
	}

	function handleChartClick(event: ECElementEvent, title: string, chartInstance: EChartsType) {
		if (event.data) {
			const [timestamp, clickedValue] = event.data as [string, number];
			const date = new Date(timestamp);

			// Get all series data
			const allSeries = chartInstance.getOption().series as EChartOption.Series[];

			// Filter points at the clicked timestamp and value
			const matchingPoints = allSeries.flatMap((series, index) => {
				if (Array.isArray(series.data)) {
					const point = series.data.find((item) => {
						const [itemTimestamp, itemClickedValue] = item as [string, number];

						return (
							Array.isArray(item) &&
							new Date(itemTimestamp).getTime() === date.getTime() &&
							itemClickedValue === clickedValue
						);
					});
					if (point && Array.isArray(point)) {
						return [
							{
								date: date.toLocaleString(),
								value: typeof point[1] === 'number' ? point[1].toFixed(4) : 'N/A',
								title,
								line: series.name || `Series ${index + 1}`
							}
						];
					}
				}
				return [];
			});

			selectedPoints = matchingPoints;
			isSheetOpen = true;
		}
	}

	function activateZoom(chart: EChartsType) {
		chart.dispatchAction({
			type: 'takeGlobalCursor',
			key: 'dataZoomSelect',
			dataZoomSelectActive: true
		});
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

	onMount(() => {
		allCharts.forEach(activateZoom);
	});
</script>

<h1 class="text-2xl mb-4">Model {timeseries.id}</h1>
<div class="mb-4 flex items-center justify-between">
	<Select bind:selected={dateRange} onSelectedChange={handleDateRangeChange}>
		<SelectTrigger class="w-[180px]">
			<SelectValue placeholder="Select date range" />
		</SelectTrigger>
		<SelectContent>
			{#each DATE_RANGES as range}
				<SelectItem value={range.value}>{range.label}</SelectItem>
			{/each}
		</SelectContent>
	</Select>
	{#if isZoomed}
		<Button on:click={resetZoom}>Reset Zoom</Button>
	{/if}
</div>

<CollapsibleSection title="Model Performance" bind:open={isModelPerformanceOpen}>
	<div style="width: 100%; height: 300px;">
		<EChart
			option={performanceChartOption}
			bind:chartInstance={performanceChart}
			on:datazoom={handleZoom}
			enableMousemove={false}
		/>
	</div>
</CollapsibleSection>

<CollapsibleSection title="Model Drift" bind:open={isModelDriftOpen}>
	<div style="width: 100%; height: 300px;">
		<EChart
			option={driftChartOption}
			bind:chartInstance={driftChart}
			on:datazoom={handleZoom}
			enableMousemove={false}
		/>
	</div>
</CollapsibleSection>

<CollapsibleSection title="Feature Monitoring" bind:open={isFeatureMonitoringOpen}>
	<Tabs value="monitoring" class="w-full">
		<TabsList>
			<TabsTrigger value="features" class="flex items-center">
				<Table class="w-4 h-4 mr-2" />
				Features
			</TabsTrigger>
			<TabsTrigger value="monitoring" class="flex items-center">
				<BarChart class="w-4 h-4 mr-2" />
				Monitoring
			</TabsTrigger>
		</TabsList>
		<TabsContent value="features">
			<div class="p-4">
				<h3 class="text-lg font-semibold">Features Content</h3>
			</div>
		</TabsContent>
		<TabsContent value="monitoring">
			<div class="p-4">
				{#each joinTimeseries.items as group}
					<CollapsibleSection title={group.name} bind:open={groupSectionStates[group.name]}>
						<div style="width: 100%; height: 300px;">
							<EChart
								option={createGroupByChartOption(group.items)}
								bind:chartInstance={groupByCharts[group.name]}
								on:click={(event) =>
									handleChartClick(event.detail, group.name, groupByCharts[group.name])}
								on:datazoom={handleZoom}
								enableMousemove={true}
							/>
						</div>
					</CollapsibleSection>
				{/each}
			</div>
		</TabsContent>
	</Tabs>
</CollapsibleSection>

<Sheet bind:open={isSheetOpen}>
	<SheetContent class="flex flex-col h-full" noAnimation={true}>
		<SheetHeader>
			<SheetTitle>Point Details</SheetTitle>
			<SheetDescription>Information about the selected point(s)</SheetDescription>
		</SheetHeader>
		{#if selectedPoints.length > 0}
			<ScrollArea class="flex-grow">
				<div class="py-4">
					{#each selectedPoints as point}
						<div class="mb-4 p-4 border rounded-md shadow-sm">
							<p><strong>Chart:</strong> {point.title}</p>
							<p><strong>Line:</strong> {point.line}</p>
							<p><strong>Date:</strong> {point.date}</p>
							<p><strong>Value:</strong> {point.value}</p>
						</div>
					{/each}
				</div>
			</ScrollArea>
		{/if}
	</SheetContent>
</Sheet>

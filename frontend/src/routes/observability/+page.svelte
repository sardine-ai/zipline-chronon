<script lang="ts">
	import EChart from '$lib/components/EChart/EChart.svelte';
	import SplitView from '$lib/components/SplitView/SplitView.svelte';
	import Button from '$lib/components/ui/button/button.svelte';
	import { generateXAxis, generateYAxis, generateHeatmapData } from '$lib/util/heatmap-data-gen';
	import type { ECElementEvent, EChartOption, EChartsType } from 'echarts';
	import { onMount } from 'svelte';

	let chart: EChartsType | null = $state(null);

	const xAxis = generateXAxis(100);
	const yAxis = generateYAxis(30);
	let data: number[][] = $state([]);
	const option: EChartOption = $derived({
		xAxis: {
			type: 'category',
			data: xAxis
		},
		yAxis: {
			type: 'category',
			data: yAxis
		},
		visualMap: [
			{
				min: 0,
				max: 1
			}
		],
		dataZoom: [
			{
				type: 'slider',
				xAxisIndex: 0,
				start: 0,
				end: 100
			},
			{
				type: 'slider',
				yAxisIndex: 0,
				start: 0,
				end: 100
			},
			{
				type: 'inside',
				xAxisIndex: 0,
				start: 0,
				end: 100
			},
			{
				type: 'inside',
				yAxisIndex: 0,
				start: 0,
				end: 100
			}
		],
		series: [
			{
				name: 'Heatmap',
				type: 'heatmap',
				data: data,
				emphasis: {
					itemStyle: {
						shadowBlur: 2
					}
				}
			}
		]
	});

	function generateAnomalousData() {
		data = generateHeatmapData(xAxis, yAxis, {
			type: 'anomalous',
			affectedRows: [28, 29, 30]
		});
	}

	function generateSlowDriftData() {
		data = generateHeatmapData(xAxis, yAxis, {
			type: 'slow-drift',
			affectedRows: [28, 29, 30]
		});
	}

	let sidebarOpen = $state(false);
	let clickedCellData: ECElementEvent['data'] | null = $state(null);

	function handleChartClick(event: CustomEvent<ECElementEvent>) {
		clickedCellData = event.detail.data;
		sidebarOpen = true;
	}

	onMount(() => {
		generateAnomalousData();
	});
</script>

<div>
	<Button on:click={generateAnomalousData}>Generate Anomalous Data</Button>
	<Button on:click={generateSlowDriftData}>Generate Slow Drift Data</Button>
</div>

<div>
	ECharts heatmaps are slow when there are lots of cells. I'm figuring it out/testing optimizaiton
	stuff
</div>

<SplitView bind:sidebarOpen>
	{#snippet main()}
		<div style="height: 600px;">
			<EChart {option} on:click={handleChartClick} bind:chartInstance={chart}></EChart>
		</div>
	{/snippet}

	{#snippet sidebar()}
		<div>
			<div>Cell Details</div>
			<div>Information about the clicked cell</div>
			{#if clickedCellData}
				<div>
					<p>X: {Array.isArray(clickedCellData) ? clickedCellData[0] : ''}</p>
					<p>Y: {Array.isArray(clickedCellData) ? clickedCellData[1] : ''}</p>
					<p>Value: {Array.isArray(clickedCellData) ? clickedCellData[2] : ''}</p>
				</div>
			{/if}
		</div>
	{/snippet}
</SplitView>

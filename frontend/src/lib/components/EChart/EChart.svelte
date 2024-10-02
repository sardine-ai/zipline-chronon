<script lang="ts">
	import { onMount, onDestroy, createEventDispatcher } from 'svelte';
	import * as echarts from 'echarts';
	import type { ECElementEvent, EChartOption } from 'echarts';

	const { option }: { option: EChartOption } = $props();
	const dispatch = createEventDispatcher();

	let chartDiv: HTMLDivElement;
	let chartInstance: echarts.ECharts;
	let resizeObserver: ResizeObserver;

	function initChart() {
		if (!chartDiv) return;

		chartInstance?.dispose();
		chartInstance = echarts.init(chartDiv);
		chartInstance.setOption(option);
		chartInstance.on('click', (params: ECElementEvent) => dispatch('click', params));
	}

	function handleResize() {
		chartInstance?.resize();
	}

	onMount(() => {
		initChart();
		resizeObserver = new ResizeObserver(handleResize);
		resizeObserver.observe(chartDiv);
	});

	onDestroy(() => {
		chartInstance?.dispose();
		resizeObserver?.disconnect();
	});

	$effect(() => {
		chartInstance?.setOption(option, true);
	});
</script>

<div bind:this={chartDiv} style="width: 100%; height: 100%;"></div>

<script lang="ts">
	import { onMount, onDestroy, createEventDispatcher } from 'svelte';
	import * as echarts from 'echarts';
	import type { ECElementEvent, EChartOption } from 'echarts';
	import merge from 'lodash/merge';

	let {
		option,
		chartInstance = $bindable(),
		enableMousemove = false,
		enableCustomZoom = false,
		width = '100%',
		height = '200px'
	}: {
		option: EChartOption;
		chartInstance: echarts.ECharts | null;
		enableMousemove?: boolean;
		enableCustomZoom?: boolean;
		width?: string;
		height?: string;
	} = $props();
	const dispatch = createEventDispatcher();

	let chartDiv: HTMLDivElement;
	let resizeObserver: ResizeObserver;

	// todo this needs to change dynamically when we support light mode
	const theme = $derived('dark');
	const defaultOption: EChartOption = $derived.by(() => ({
		backgroundColor: 'transparent',
		...(enableCustomZoom && {
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
		}),
		textStyle: {
			fontFamily: 'Geist Mono, Geist'
		}
	}));

	const mergedOption: EChartOption = $derived.by(() => {
		return merge({}, defaultOption, option);
	});

	function initChart() {
		if (!chartDiv) return;

		chartInstance?.dispose();
		chartInstance = echarts.init(chartDiv, theme);
		chartInstance.setOption(mergedOption);
		chartInstance.on('click', (params: ECElementEvent) => dispatch('click', params));
		chartInstance.on('datazoom', (params: EChartOption.DataZoom) => dispatch('datazoom', params));

		if (enableMousemove) {
			chartInstance.on('mousemove', (params: ECElementEvent) => {
				if (params.componentType === 'series') {
					chartInstance?.getZr().setCursorStyle('pointer');
				}
			});
		}

		if (enableCustomZoom) {
			activateZoom();
		}
	}

	function activateZoom() {
		chartInstance?.dispatchAction({
			type: 'takeGlobalCursor',
			key: 'dataZoomSelect',
			dataZoomSelectActive: true
		});
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
		chartInstance?.setOption(mergedOption);
	});
</script>

<div style="width: {width}; height: {height};">
	<div bind:this={chartDiv} style="width: 100%; height: 100%;"></div>
</div>

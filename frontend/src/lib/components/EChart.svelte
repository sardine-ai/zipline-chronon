<script lang="ts">
	import { onMount, onDestroy, createEventDispatcher } from 'svelte';
	import * as echarts from 'echarts';
	import type { ECElementEvent, EChartOption } from 'echarts';
	import merge from 'lodash/merge';
	import EChartTooltip from '$lib/components/EChartTooltip.svelte';
	import CustomEChartLegend from '$lib/components/CustomEChartLegend.svelte';

	let {
		option,
		chartInstance = $bindable(),
		enableMousemove = false,
		enableCustomZoom = false,
		width = '100%',
		height = '230px',
		enableCustomTooltip = false,
		enableTooltipClick = false,
		showCustomLegend = false,
		legendGroup = undefined,
		markPoint = undefined
	}: {
		option: EChartOption;
		chartInstance: echarts.ECharts | null;
		enableMousemove?: boolean;
		enableCustomZoom?: boolean;
		width?: string;
		height?: string;
		enableCustomTooltip?: boolean;
		enableTooltipClick?: boolean;
		showCustomLegend?: boolean;
		legendGroup?: { name: string; items: Array<{ feature: string }> };
		markPoint?: ECElementEvent;
	} = $props();
	const dispatch = createEventDispatcher();

	let chartDiv: HTMLDivElement;
	let resizeObserver: ResizeObserver;

	// todo this needs to change dynamically when we support light mode
	const theme = $derived('dark');
	const defaultOption: EChartOption = $derived.by(() => ({
		backgroundColor: 'transparent',
		animationDuration: 300,
		animationDurationUpdate: 300,
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
		if (!enableCustomTooltip) {
			return merge({}, defaultOption, option);
		}

		const customTooltipOption = {
			tooltip: {
				formatter: () => '',
				trigger: 'axis',
				axisPointer: {
					type: 'line'
				},
				position: 'top'
			}
		};

		return merge({}, defaultOption, option, customTooltipOption);
	});

	let tooltipData = $state<{
		xValue: number | null;
		series: Array<{
			name: string | undefined;
			value: number;
			color: string;
		}>;
	}>({ xValue: null, series: [] });
	let isTooltipVisible = $state(false);
	let isCommandPressed = $state(false);
	let isMouseOverTooltip = $state(false);
	let hideTimeoutId: ReturnType<typeof setTimeout>;

	const isBarChart = $derived.by(() => {
		const series = mergedOption.series as EChartOption.Series[];
		return series?.some((s) => s.type === 'bar');
	});

	function handleKeyDown(event: KeyboardEvent) {
		if ((event.metaKey || event.ctrlKey) && event.type === 'keydown') {
			isCommandPressed = true;
			disableChartInteractions();

			if (exactX !== null && chartInstance) {
				const option = chartInstance.getOption();
				const series = option.series as EChartOption.Series[];
				const firstSeries = series[0];

				if (Array.isArray(firstSeries.data)) {
					// Find the index of the point with matching x-value
					const dataIndex = firstSeries.data.findIndex(
						(point) => (point as [number, number])[0] === exactX
					);

					if (dataIndex !== -1) {
						// for some reason, we need to showTip somewhere else first
						chartInstance.dispatchAction({
							type: 'showTip',
							seriesIndex: 0,
							dataIndex: -1
						});
						chartInstance.dispatchAction({
							type: 'showTip',
							seriesIndex: 0,
							dataIndex: dataIndex
						});
					}
				}
			}
		}
	}

	function handleKeyUp(event: KeyboardEvent) {
		if (event.key === 'Meta' || event.key === 'Control') {
			isCommandPressed = false;
			enableChartInteractions();
			if (!isMouseOverTooltip) {
				isTooltipVisible = false;
			}
		}
	}

	function disableChartInteractions() {
		chartInstance?.setOption({
			triggerOn: 'none'
		} as EChartOption);
	}

	function enableChartInteractions() {
		chartInstance?.setOption({
			triggerOn: 'mousemove'
		} as EChartOption);
	}

	function resetChartPointer() {
		if (!chartInstance) return;

		// Get the zrender instance
		const zr = chartInstance.getZr();
		if (!zr) return;

		// Simulate mouseout
		zr.handler.dispatch('mouseout', {
			type: 'mouseout'
		});
	}

	function initChart() {
		if (!chartDiv) return;

		chartInstance?.dispose();
		chartInstance = echarts.init(chartDiv, theme);
		chartInstance.setOption(mergedOption);

		chartInstance.on('click', (params: ECElementEvent) => {
			dispatch('click', {
				detail: params,
				fromTooltip: false
			});
			resetChartPointer();
		});
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

		if (enableCustomTooltip) {
			setupCustomTooltip();
		}
	}

	function activateZoom() {
		chartInstance?.dispatchAction({
			type: 'takeGlobalCursor',
			key: 'dataZoomSelect',
			dataZoomSelectActive: true
		});
	}

	function handleTooltipMouseEnter() {
		if (hideTimeoutId) {
			clearTimeout(hideTimeoutId);
		}
		isMouseOverTooltip = true;
	}

	function handleTooltipMouseLeave() {
		isMouseOverTooltip = false;
		hideTooltip();
	}

	function hideTooltip() {
		hideTimeoutId = setTimeout(() => {
			if (!isMouseOverTooltip && !isCommandPressed) {
				isTooltipVisible = false;
			}
		}, 150);
	}

	function setupCustomTooltip() {
		if (!chartInstance || !enableCustomTooltip) return;

		const zr = chartInstance.getZr();
		zr.on('mousemove', showTooltip);
		zr.on('globalout', hideTooltip);
	}

	let exactX = $state<number | null>(null);

	function showTooltip(params: { offsetX: number; offsetY: number }) {
		if (isCommandPressed) return;

		if (hideTimeoutId) {
			clearTimeout(hideTimeoutId);
		}

		const pointInPixel = [params.offsetX, params.offsetY];
		const pointInGrid = chartInstance!.convertFromPixel({ seriesIndex: 0 }, pointInPixel);

		if (!pointInGrid || !chartInstance) return;

		const option = chartInstance.getOption();
		const series = option.series as EChartOption.Series[];
		const colors = option.color as string[];

		if (isBarChart) {
			// Handle stacked bar chart
			const xAxis = option.xAxis as EChartOption.XAxis;
			if (!Array.isArray(xAxis)) return;

			const categories = xAxis[0].data as string[];
			if (!categories) return;

			// Convert mouse position to domain coordinates
			const xValue = pointInGrid[0];

			// Find the nearest valid category index
			const categoryIndex = Math.round(xValue);

			// Only show tooltip if we're at a valid category index
			if (categoryIndex >= 0 && categoryIndex < categories.length) {
				const seriesData = series
					.map((s, index) => {
						if (!Array.isArray(s.data)) return null;
						return {
							name: s.name,
							value: s.data[categoryIndex] as number,
							color: colors[index]
						};
					})
					.filter((item): item is NonNullable<typeof item> => item !== null);

				if (seriesData.length > 0) {
					tooltipData = {
						xValue: categoryIndex,
						series: seriesData
					};
					isTooltipVisible = true;
				} else {
					isTooltipVisible = false;
				}
			} else {
				isTooltipVisible = false;
			}
		} else {
			// Find the nearest x-coordinate that exists in the data
			const firstSeries = series[0];
			if (!Array.isArray(firstSeries.data)) return;

			const nearestPoint = firstSeries.data.reduce((prev, curr) => {
				const [prevX] = prev as [number, number];
				const [currX] = curr as [number, number];
				return Math.abs(currX - pointInGrid[0]) < Math.abs(prevX - pointInGrid[0]) ? curr : prev;
			});

			exactX = (nearestPoint as [number, number])[0];

			// Get values for all series at this exact x-coordinate
			const seriesData = series
				.map((s, index) => {
					if (!Array.isArray(s.data)) return null;

					const exactPoint = s.data.find((point) => (point as [number, number])[0] === exactX);
					if (!exactPoint) return null;

					return {
						name: s.name,
						value: (exactPoint as [number, number])[1],
						color: colors[index]
					};
				})
				.filter((item): item is NonNullable<typeof item> => item !== null);

			// Only show tooltip if we found matching data points
			if (seriesData.length > 0) {
				tooltipData = {
					xValue: exactX,
					series: seriesData
				};
				isTooltipVisible = true;
			} else {
				isTooltipVisible = false;
			}
		}
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
		if (hideTimeoutId) {
			clearTimeout(hideTimeoutId);
		}
	});

	$effect(() => {
		if (!chartInstance) return;

		const updatedOption = { ...mergedOption };

		// Add mark point series if provided
		if (markPoint?.data) {
			const series: EChartOption.Series[] = Array.isArray(updatedOption.series)
				? [...((updatedOption.series ?? []) as EChartOption.Series[])]
				: [(updatedOption.series ?? []) as EChartOption.Series];

			series.push({
				type: 'line',
				markLine: {
					silent: true,
					symbol: [false, 'circle'],
					lineStyle: {
						color: '#ffffff',
						type: [8, 8],
						width: 1
					},
					emphasis: {
						disabled: true
					},
					data: [
						[
							{ xAxis: (markPoint.data as number[])[0], yAxis: 0 },
							{ xAxis: (markPoint.data as number[])[0], yAxis: (markPoint.data as number[])[1] }
						]
					]
				},
				z: 10,
				animation: false
			} as EChartOption.Series);

			updatedOption.series = series;
		}

		chartInstance.setOption(updatedOption);
	});

	function handleDoubleClick() {
		chartInstance?.dispatchAction({
			type: 'dataZoom',
			start: 0,
			end: 100
		});
	}

	function handleWindowBlur() {
		isCommandPressed = false;
		enableChartInteractions();
		if (!isMouseOverTooltip) {
			isTooltipVisible = false;
		}
	}
</script>

<svelte:window on:keydown={handleKeyDown} on:keyup={handleKeyUp} on:blur={handleWindowBlur} />

<div style="width: {width}; height: {height};">
	<div
		bind:this={chartDiv}
		ondblclick={handleDoubleClick}
		role="application"
		style="width: 100%; height: 100%;"
	></div>
	{#if enableCustomTooltip}
		<div
			class="relative z-50 inline-block"
			role="tooltip"
			onmouseenter={handleTooltipMouseEnter}
			onmouseleave={handleTooltipMouseLeave}
		>
			<EChartTooltip
				visible={isTooltipVisible}
				xValue={tooltipData.xValue}
				series={tooltipData.series}
				clickable={enableTooltipClick}
				xAxisCategories={isBarChart && chartInstance
					? ((chartInstance.getOption()?.xAxis as EChartOption.XAxis[])?.[0]?.data as string[])
					: undefined}
				chart={chartInstance}
				on:click={(event) =>
					dispatch('click', {
						detail: event.detail,
						fromTooltip: true
					})}
			/>
		</div>
	{/if}
</div>
{#if showCustomLegend && legendGroup && chartInstance}
	<CustomEChartLegend
		groupName={legendGroup.name}
		items={legendGroup.items}
		chart={chartInstance}
	/>
{/if}

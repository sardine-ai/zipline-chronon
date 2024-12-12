<script lang="ts">
	import EChart from '$lib/components/EChart/EChart.svelte';
	import type { EChartOption, EChartsType } from 'echarts';
	import type { TimeSeriesItem } from '$lib/types/Model/Model';
	import { createChartOption } from '$lib/util/chart-options.svelte';

	let {
		data = null,
		chartInstance = $bindable()
	}: { data: TimeSeriesItem[] | null; chartInstance: EChartsType | null } = $props();

	const targetLabels = ['p5', 'p50', 'p95'];

	function getPercentileChartSeries(data: TimeSeriesItem[]): EChartOption.Series[] {
		return targetLabels
			.filter((label) => data.some((point) => point.label === label))
			.map((label) => ({
				name: label,
				type: 'line',
				emphasis: {
					focus: 'series'
				},
				data: data.filter((point) => point.label === label).map((point) => [point.ts, point.value])
			})) as EChartOption.Series[];
	}

	function getPercentileChartOption(data: TimeSeriesItem[]): EChartOption {
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

	let percentileChartOption = $derived.by(() => {
		if (!data) return {};
		return getPercentileChartOption(data);
	});
</script>

<EChart
	option={percentileChartOption}
	bind:chartInstance
	enableMousemove={false}
	enableCustomZoom={true}
	enableCustomTooltip={true}
/>

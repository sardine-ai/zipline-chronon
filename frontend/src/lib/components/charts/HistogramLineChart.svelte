<script lang="ts">
	import { onMount, type ComponentProps } from 'svelte';
	import { scaleOrdinal } from 'd3';

	import CustomLineChart from './CustomLineChart.svelte';
	import { colors } from './common';
	import type { ITileSummarySeriesArgs } from '$src/lib/types/codegen';
	import { NULL_VALUE } from '$src/lib/constants/common';
	import { sortFunc } from '@layerstack/utils';

	type CustomLineChartProps = ComponentProps<typeof CustomLineChart>;

	type Props = {
		data: ITileSummarySeriesArgs;
		profile?: boolean;
	} & Omit<CustomLineChartProps, 'data' | 'series'>;

	let { data, profile, ...restProps }: Props = $props();

	const keys = $derived(Object.keys(data.histogram ?? {}).sort());
	const colorScale = $derived(scaleOrdinal<string>().domain(keys).range(colors));

	// Note: Using `$derived()` or defining inline causes performance issue (likely due to creating proxies for deep reactivity).
	const series = $derived(
		Object.entries(data.histogram ?? {})
			.map(([key, values], i) => {
				const timestamps = data.timestamps ?? [];

				return {
					key,
					data: timestamps.map((ts, i) => {
						const value = values[i];
						return {
							date: new Date(ts as number),
							value: value === NULL_VALUE ? null : value
						};
					}),
					color: colorScale(key)
				};
			})
			.sort(sortFunc('key'))
	);

	if (profile) {
		console.time('HistogramLineChart render');
		onMount(() => {
			console.timeEnd('HistogramLineChart render');
		});
	}
</script>

<CustomLineChart {series} format="metric" {...restProps} />

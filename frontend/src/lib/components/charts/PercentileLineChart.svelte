<script lang="ts">
	import { onMount, type ComponentProps } from 'svelte';

	import CustomLineChart from './CustomLineChart.svelte';
	import type { ITileSummarySeriesArgs } from '$src/lib/types/codegen';
	import { NULL_VALUE } from '$src/lib/constants/common';

	type CustomLineChartProps = ComponentProps<typeof CustomLineChart>;

	type Props = {
		data: ITileSummarySeriesArgs;
		profile?: boolean;
	} & Omit<CustomLineChartProps, 'data' | 'series'>;

	let { data, profile, ...restProps }: Props = $props();

	// Using `$derived()` or defining inline causes performance issue (likely due to creating proxies for deep reactivity).
	const series = $derived(
		[
			{ label: 'p95', color: '#2976E6', index: 2 },
			{ label: 'p50', color: '#3DDC91', index: 1 },
			{ label: 'p5', color: '#E5B72D', index: 0 }
		].map((c) => {
			const timestamps = data.timestamps ?? [];
			const values = data.percentiles?.[c.index] ?? [];

			return {
				key: c.label,
				data: timestamps.map((ts, i) => {
					const value = values[i];
					return {
						date: new Date(ts as number),
						value: value === NULL_VALUE ? null : value
					};
				}),
				color: c.color
			};
		})
	);

	if (profile) {
		console.time('PercentileLineChart render');
		onMount(() => {
			console.timeEnd('PercentileLineChart render');
		});
	}
</script>

<CustomLineChart {series} format="metric" {...restProps} />

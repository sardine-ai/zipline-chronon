<script lang="ts">
	import type { ComponentProps } from 'svelte';
	import { LineChart } from 'layerchart';
	import merge from 'lodash/merge';

	import { lineChartProps } from './common';
	import type { TimeSeriesItem } from '$lib/types/Model/Model';

	type LineChartProps = ComponentProps<typeof LineChart>;
	type BrushProps = Exclude<LineChartProps['brush'], undefined | boolean>;

	type Props = {
		data: TimeSeriesItem[];
		onbrushend?: BrushProps['onbrushend'];
	} & Omit<LineChartProps, 'data'>;

	let { data, onbrushend, ...restProps }: Props = $props();
</script>

<LineChart
	x="date"
	y="value"
	series={[
		{ label: 'p95', color: '#4B92FF' },
		{ label: 'p50', color: '#7DFFB3' },
		{ label: 'p5', color: '#FDDD61' }
	].map((c) => {
		return {
			key: c.label,
			data:
				data
					?.filter((d) => d.label === c.label)
					.map((d) => {
						return {
							date: new Date(d.ts),
							value: d.value
						};
					}) ?? [],
			color: c.color
		};
	})}
	padding={{ left: 36, bottom: 20 }}
	brush={{ onbrushend }}
	{...merge(
		{},
		lineChartProps,
		{
			props: {
				yAxis: { format: 'metric' }
			}
		},
		restProps
	)}
/>

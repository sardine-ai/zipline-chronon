<script lang="ts">
	import type { ComponentProps } from 'svelte';
	import { LineChart } from 'layerchart';
	import merge from 'lodash/merge';

	import { lineChartProps } from './common';
	import type { ITileSummarySeries } from '$src/lib/types/codegen';
	import { zip } from 'd3';
	import { Int64 } from '@creditkarma/thrift-server-core';
	import { NULL_VALUE } from '$src/lib/constants/common';

	type LineChartProps = ComponentProps<typeof LineChart>;
	type BrushProps = Exclude<LineChartProps['brush'], undefined | boolean>;

	type Props = {
		data: ITileSummarySeries;
		onbrushend?: BrushProps['onbrushend'];
	} & Omit<LineChartProps, 'data'>;

	let { data, onbrushend, ...restProps }: Props = $props();
</script>

<LineChart
	x="date"
	y="value"
	series={[
		{ label: 'p95', color: '#4B92FF', index: 19 },
		{ label: 'p50', color: '#7DFFB3', index: 10 },
		{ label: 'p5', color: '#FDDD61', index: 1 }
	].map((c) => {
		const timestamps = data.timestamps ?? [];
		const values = data.percentiles?.[c.index] ?? [];

		return {
			key: c.label,
			data: zip<Int64 | number>(timestamps, values).map(([ts, value]) => {
				return {
					date: new Date(ts as number),
					value: value === NULL_VALUE ? null : value
				};
			}),
			color: c.color
		};
	})}
	padding={{ top: 4, left: 36, bottom: 20 }}
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

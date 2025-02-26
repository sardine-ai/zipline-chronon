<script lang="ts">
	import type { ComponentProps } from 'svelte';
	import { accessor, Circle, findRelatedData, Line, LineChart, Tooltip } from 'layerchart';
	import { lineChartProps, tooltipProps, type DateValue } from './common';
	import type { ITileDriftSeriesArgs } from '$src/lib/types/codegen';
	import { formatDate, formatValue } from '$lib/util/format';
	import { Badge } from '$lib/components/ui/badge';
	import { isMacOS } from '$src/lib/util/browser';
	import { transformSeries, getColumns } from '$lib/util/series';

	type LineChartProps = ComponentProps<typeof LineChart>;
	type BrushProps = Exclude<LineChartProps['brush'], undefined | boolean>;

	type Props = {
		data: ITileDriftSeriesArgs[];
		markPoint?: DateValue;
		onitemclick?: (item: {
			series: NonNullable<LineChartProps['series']>[number];
			data: DateValue;
			value: number;
		}) => void;
		onbrushend?: BrushProps['onbrushend'];
	} & Omit<LineChartProps, 'data'>;

	let { data, markPoint, onitemclick, onbrushend, ...restProps }: Props = $props();

	const columns = $derived(getColumns(data));
</script>

<LineChart
	x="date"
	y="value"
	series={data.map((d) => transformSeries(d, columns))}
	padding={{ top: 4, left: 36, bottom: 48 }}
	legend={{
		placement: 'bottom-left',
		classes: {
			root: 'right-0 overflow-auto scrollbar-none',
			swatch: 'h-2 w-2 mt-[4px] mr-1 rounded-full',
			label: 'text-sm'
			// item: () => 'flex items-center gap-2'
		}
	}}
	renderContext="canvas"
	{...lineChartProps}
	{...restProps}
	brush={{ onbrushend }}
	tooltip={{
		hideDelay: 150,
		...(typeof restProps.tooltip === 'object' ? restProps.tooltip : null)
	}}
>
	<svelte:fragment slot="aboveMarks" let:xScale let:yScale>
		{#if markPoint}
			{@const x = xScale(markPoint.date)}
			{@const y = yScale(markPoint.value)}
			<Line
				x1={x}
				y1={yScale(0)}
				x2={x}
				y2={y}
				class="stroke-surface-content [stroke-dasharray:4,4]"
			/>
			<Circle cx={x} cy={y} r={4} class="fill-surface-content" />
		{/if}
	</svelte:fragment>

	<svelte:fragment
		slot="tooltip"
		let:x
		let:y
		let:height
		let:visibleSeries
		let:setHighlightSeriesKey
	>
		<Tooltip.Root {...tooltipProps.root} x="data" y={height + 24} pointerEvents let:data>
			<Tooltip.Header {...tooltipProps.header}>
				{formatDate(x(data))}
			</Tooltip.Header>

			<Tooltip.List {...tooltipProps.list} class="px-1 pb-1 gap-y-0">
				{#each visibleSeries as s}
					{@const seriesTooltipData = s.data ? findRelatedData(s.data, data, x) : data}
					{@const valueAccessor = accessor(s.value ?? (s.data ? (y as unknown) : s.key))}
					{@const value = seriesTooltipData ? valueAccessor(seriesTooltipData) : null}

					<button
						class="col-span-full grid grid-cols-[1fr_auto] gap-6 hover:bg-neutral-400 py-2 px-3 rounded"
						onclick={() => onitemclick?.({ series: s, data: seriesTooltipData, value })}
						onmouseenter={() => setHighlightSeriesKey(s.key)}
						onmouseleave={() => setHighlightSeriesKey(null)}
					>
						<Tooltip.Item
							label={s.label ?? (s.key !== 'default' ? s.key : 'value')}
							{value}
							color={s.color}
							format={formatValue}
							{...tooltipProps.item}
						/>
					</button>
				{/each}
			</Tooltip.List>

			<div class="text-foreground text-xs px-3 py-2 border-t">
				<Badge variant="key">{isMacOS() ? '⌘' : 'Ctrl'}</Badge> to lock tooltip
			</div>
		</Tooltip.Root>
	</svelte:fragment>
</LineChart>

<script lang="ts">
	import { onMount, type ComponentProps } from 'svelte';
	import {
		accessor,
		Circle,
		findRelatedData,
		Group,
		Line,
		LinearGradient,
		LineChart,
		Pattern,
		Rect,
		Rule,
		Spline,
		Svg,
		Text,
		Tooltip,
		type SeriesData
	} from 'layerchart';
	import { Kbd } from 'svelte-ux';
	import { cls } from '@layerstack/tailwind';
	import { format as formatUtil, PeriodType, type FormatType } from '@layerstack/utils';

	import { lineChartProps, tooltipProps, type DateValue } from './common';
	import { isMacOS } from '$src/lib/util/browser';

	type LineChartProps = ComponentProps<typeof LineChart>;
	type BrushProps = Exclude<LineChartProps['brush'], undefined | boolean>;

	type Props = {
		series: NonNullable<LineChartProps['series']>;
		onItemClick?: (item: {
			series: NonNullable<LineChartProps['series']>[number];
			data: DateValue;
			value: number;
		}) => void;
		onBrushEnd?: BrushProps['onBrushEnd'];
		profile?: boolean;
		format?: FormatType;
		tooltipFormat?: FormatType;

		thresholds?: { label: string; value: number }[];
		annotations?: Array<
			| {
					type: 'point';
					label?: string;
					x?: Date;
					y?: number;
					seriesKey?: string;
					layer?: 'above' | 'below';
					classes?: {
						label?: string;
						circle?: string;
					};
			  }
			| {
					type: 'line';
					x: Date;
					y?: number;
					seriesKey?: string;
					layer?: 'above' | 'below';
			  }
			| {
					type: 'range';
					x: Date[] | [Date, Date];
					seriesKey?: string;
					layer?: 'above' | 'below';
					gradient?: ComponentProps<typeof LinearGradient>;
					pattern?: ComponentProps<typeof Pattern>;
			  }
		>;

		// TODO: Use restProps once reactivity issue (brush/tooltip locking resetting context) is resolved
		lockedTooltip?: boolean;
		xDomain?: LineChartProps['xDomain'];
		yDomain?: LineChartProps['yDomain'];
		onPointClick?: (
			e: MouseEvent,
			details: {
				data: DateValue;
				series: SeriesData<{ date: Date; value: number | null }, typeof Spline>;
			}
		) => void;
		ref?: LineChartProps['ref'];
	} & Omit<LineChartProps, 'data'>;

	let {
		series,
		onItemClick,
		onBrushEnd,
		profile,
		lockedTooltip,
		format,
		tooltipFormat,

		thresholds = [],
		annotations = [],

		// ref,
		xDomain,
		yDomain,
		onPointClick,
		...restProps
	}: Props = $props();

	if (profile) {
		console.time('CustomLineChart render');
		onMount(() => {
			console.timeEnd('CustomLineChart render');
		});
	}
</script>

<LineChart
	x="date"
	y="value"
	{xDomain}
	{yDomain}
	{series}
	padding={{ top: 8, left: 36, bottom: 48 }}
	legend={{
		placement: 'bottom-left',
		classes: {
			root: 'right-0 left-8 overflow-auto scrollbar-none',
			swatch: 'h-2 w-2 mt-[4px] mr-1 rounded-full',
			label: 'text-sm'
			// item: () => 'flex items-center gap-2'
		}
	}}
	brush={{ onBrushEnd }}
	renderContext="canvas"
	{...restProps}
	props={{
		...lineChartProps.props,
		yAxis: {
			...lineChartProps.props.yAxis,
			format: format === 'percent' ? 'percentRound' : format
		},
		tooltip: {
			context: {
				hideDelay: 150,
				locked: lockedTooltip
			}
		}
	}}
	onPointClick={onPointClick as any}
>
	{#snippet belowContext(snippetProps)}
		<!-- Using <Svg> layer until Pattern supports Canvas -->
		<Svg>
			{@render annotationsSnippet(snippetProps, 'below')}
		</Svg>

		{#if restProps.belowContext}
			{@render restProps.belowContext(snippetProps)}
		{/if}
	{/snippet}

	{#snippet aboveContext(snippetProps)}
		{@const { context } = snippetProps}

		<!-- Using <Svg> layer until Pattern supports Canvas -->
		<Svg pointerEvents={false}>
			{@render annotationsSnippet(snippetProps, 'above')}

			{#each thresholds as threshold}
				<Pattern size={8} lines={{ rotate: -45, opacity: 0.1 }}>
					{#snippet children({ pattern })}
						<Rect
							x={0}
							y={0}
							width={context.width}
							height={context.yScale(threshold.value)}
							fill={pattern}
						/>
					{/snippet}
				</Pattern>

				<Rect
					x={-context.padding.left}
					y={context.yScale(threshold.value) - 8}
					width={context.padding.left}
					height={16}
					rx={4}
					class="stroke-neutral-600 fill-neutral-100"
				/>
				<Text
					value={threshold.label}
					x={-context.padding.left / 2}
					y={context.yScale(threshold.value)}
					textAnchor="middle"
					verticalAnchor="middle"
					dy={-2}
					class="fill-neutral-800 font-bold text-[10px]"
				/>

				<Rule y={threshold.value} class="stroke-neutral-500 [stroke-dasharray:4,4]" />
			{/each}
		</Svg>

		{#if restProps.aboveContext}
			{@render restProps.aboveContext(snippetProps)}
		{/if}
	{/snippet}

	{#snippet tooltip({ context, visibleSeries, highlightKey, setHighlightKey })}
		{@const data = context.tooltip.data}

		<Tooltip.Root {...tooltipProps.root} x="data" y={context.height + 24} pointerEvents>
			<Tooltip.Header {...tooltipProps.header}>
				{data ? formatUtil(context.x(data), PeriodType.DayTime) : ''}
			</Tooltip.Header>

			<Tooltip.List {...tooltipProps.list} class="px-1 pb-1 gap-y-0">
				{#each visibleSeries as s}
					{@const seriesTooltipData = s.data ? findRelatedData(s.data, data, context.x) : data}
					{@const valueAccessor = accessor(s.value ?? (s.data ? context.y : s.key))}
					{@const value = seriesTooltipData ? valueAccessor(seriesTooltipData) : null}

					<button
						class={cls(
							'col-span-full grid grid-cols-[1fr_auto] gap-6 hover:bg-neutral-400 py-2 px-3 rounded',
							highlightKey !== null && s.key !== highlightKey && 'opacity-20'
						)}
						onclick={() => onItemClick?.({ series: s as any, data: seriesTooltipData, value })}
						onmouseenter={() => setHighlightKey(s.key)}
						onmouseleave={() => setHighlightKey(null)}
					>
						<Tooltip.Item
							label={s.label ?? (s.key !== 'default' ? s.key : 'value')}
							{value}
							color={s.color}
							format={tooltipFormat ?? (format === 'metric' ? 'integer' : format) ?? formatUtil}
							{...tooltipProps.item}
						/>
					</button>
				{/each}
			</Tooltip.List>

			<div class="text-foreground text-xs px-3 py-2 border-t">
				<Kbd
					command={isMacOS()}
					control={!isMacOS()}
					class="size-5 p-0 items-center justify-center border border-neutral-500 mr-1"
				/> to lock tooltip
			</div>
		</Tooltip.Root>
	{/snippet}
</LineChart>

{#snippet annotationsSnippet(
	snippetProps: Parameters<NonNullable<typeof restProps.aboveMarks>>[0],
	layer: 'above' | 'below'
)}
	{@const { context, highlightKey, visibleSeries } = snippetProps}

	{@const visibleAnnotations = annotations.filter(
		(a) =>
			(a.layer === layer || (layer === 'above' && a.layer == null)) &&
			(highlightKey == null || a.seriesKey == null || a.seriesKey === highlightKey) &&
			visibleSeries.some((s) => a.seriesKey == null || a.seriesKey === s.key)
	)}

	{#each visibleAnnotations as annotation}
		{#if annotation.type === 'range'}
			{#if annotation.gradient}
				<LinearGradient {...annotation.gradient}>
					{#snippet children({ gradient })}
						<Rect
							x={context.xScale(annotation.x[0])}
							y={0}
							width={context.xScale(annotation.x[1]) - context.xScale(annotation.x[0])}
							height={context.height}
							fill={gradient}
						/>
					{/snippet}
				</LinearGradient>
			{/if}

			{#if annotation.pattern}
				<Pattern {...annotation.pattern}>
					{#snippet children({ pattern })}
						<Rect
							x={context.xScale(annotation.x[0])}
							y={0}
							width={context.xScale(annotation.x[1]) - context.xScale(annotation.x[0])}
							height={context.height}
							fill={pattern}
						/>
					{/snippet}
				</Pattern>
			{/if}
		{:else if annotation.type === 'point'}
			<Group
				x={annotation.x ? context.xScale(annotation.x) : 0}
				y={annotation.y ? context.yScale(annotation.y) : context.height}
				onpointermove={(e) => {
					e.stopPropagation();
					// context.tooltip.show(e, { anomaly });
				}}
				onpointerleave={() => {
					// context.tooltip.hide();
				}}
			>
				<Circle r={6} class={cls('stroke-surface-100', annotation.classes?.circle)} />
				{#if annotation.label}
					<Text
						value={annotation.label}
						textAnchor="middle"
						verticalAnchor="middle"
						dy={-2}
						class={cls(
							'text-[10px] fill-secondary-content font-bold pointer-events-none',
							annotation.classes?.label
						)}
					/>
				{/if}
			</Group>
		{:else if annotation.type === 'line'}
			<Line
				x1={context.xScale(annotation.x)}
				y1={context.yScale(0)}
				x2={context.xScale(annotation.x)}
				y2={context.yScale(annotation.y)}
				class="stroke-surface-content [stroke-dasharray:4,4]"
			/>
		{/if}
	{/each}
{/snippet}

<script lang="ts">
	import { BarChart, PieChart, type SeriesData } from 'layerchart';
	import { Button, Drawer, Field, TextField, Toggle, ToggleGroup, ToggleOption } from 'svelte-ux';
	import { entries, format, PeriodType, sortFunc } from '@layerstack/utils';
	import { type DomainType } from 'layerchart/utils/scales.svelte';
	import { parseISO, subDays } from 'date-fns';
	import { resource } from 'runed';
	import { scaleThreshold, randomUniform } from 'd3';
	import { queryParameters } from 'sveltekit-search-params';

	import {
		barChartProps,
		successColor,
		dangerColor,
		type DateValue,
		pieChartProps,
		tooltipProps,
		xAxisProps,
		yAxisProps
	} from '$src/lib/components/charts/common';
	import CustomLineChart from '$src/lib/components/charts/CustomLineChart.svelte';
	import HistogramLineChart from '$src/lib/components/charts/HistogramLineChart.svelte';
	import PercentileLineChart from '$src/lib/components/charts/PercentileLineChart.svelte';
	import { transformSeries } from '$src/lib/components/charts/utils';
	import CollapsibleSection from '$src/lib/components/CollapsibleSection.svelte';
	import { NULL_VALUE } from '$src/lib/constants/common';
	import type {
		ITileDriftSeriesArgs,
		ITileSeriesKeyArgs,
		ITileSummarySeriesArgs
	} from '$src/lib/types/codegen';

	import { Api } from '$src/lib/api/api';
	import { isZoomed, resetZoom, shared } from '../../../shared.svelte';
	import { isMacOS } from '$src/lib/util/browser';
	import { formatDrift } from '$src/lib/util/format';
	import ResetZoomButton from '$src/lib/components/ResetZoomButton.svelte';
	import { getOffsetParamsConfig } from '$src/lib/params/offset';
	import DateRangeField from '$src/lib/components/DateRangeField.svelte';

	import IconChevronsLeftRightEllipsis from '~icons/lucide/chevrons-left-right-ellipsis';
	import IconXMark from '~icons/heroicons/x-mark';

	let {
		tileSeriesKey,
		point,
		color,
		dateRange,
		groupDriftSeries,
		driftMetricDomain
	}: {
		tileSeriesKey: ITileSeriesKeyArgs | undefined;
		point: DateValue | undefined;
		color: string;
		dateRange: { start: Date; end: Date };
		groupDriftSeries: ITileDriftSeriesArgs[];
		driftMetricDomain: number[];
	} = $props();

	const api = new Api();

	const params = queryParameters(getOffsetParamsConfig(), {
		pushHistory: false,
		showDefaults: false,
		debounceHistory: 500
	});

	const driftSeries = $derived(
		groupDriftSeries.filter((s) => s.key?.column === tileSeriesKey?.column)[0]
	);

	const columnSummaryActualResource = resource([() => tileSeriesKey], async ([tileSeriesKey]) => {
		if (!tileSeriesKey) {
			return null;
		}

		return api.getColumnSummary({
			name: tileSeriesKey.nodeName!,
			columnName: tileSeriesKey.column!,
			startTs: dateRange.start.getTime(),
			endTs: dateRange.end.getTime()
		});
	});

	const columnSummaryBaselineResource = resource(
		[() => tileSeriesKey, () => params.offset],
		async ([tileSeriesKey, offset]) => {
			if (!tileSeriesKey) {
				return null;
			}

			return api.getColumnSummary({
				name: tileSeriesKey.nodeName!,
				columnName: tileSeriesKey.column!,
				startTs: Number(subDays(dateRange.start, offset)),
				endTs: Number(subDays(dateRange.end, offset))
			});
		}
	);

	const columnSummaryActualData = $derived(columnSummaryActualResource.current);
	const columnSummaryBaselineData = $derived(columnSummaryBaselineResource.current);

	/** Actual */
	const actualPointDate = $derived(point?.date ?? new Date());
	// Index of selected timeline point within drift series
	const actualDriftPointIndex = $derived(
		driftSeries?.timestamps?.findIndex(
			(ts) => (ts as unknown as number) === actualPointDate.getTime()
		) ?? -1
	);
	const actualPointDrift = $derived(
		driftSeries.histogramDriftSeries?.[actualDriftPointIndex] ??
			driftSeries?.percentileDriftSeries?.[actualDriftPointIndex]
	);
	// Index of selected timeline point within summary series
	const actualSummaryPointIndex = $derived(
		columnSummaryActualData?.timestamps?.findIndex(
			(ts) => (ts as unknown as number) === actualPointDate.getTime()
		) ?? -1
	);

	/** Baseline */
	const baselinePointDate = $derived(subDays(actualPointDate, params.offset));
	// Index of selected timeline point within summary series
	const baselineDriftPointIndex = $derived(
		columnSummaryActualData?.timestamps?.findIndex(
			(ts) => (ts as unknown as number) === baselinePointDate?.getTime()
		) ?? -1
	);
	const baselinePointDrift = $derived(
		driftSeries.histogramDriftSeries?.[baselineDriftPointIndex] ??
			driftSeries?.percentileDriftSeries?.[baselineDriftPointIndex]
	);
	// Index of selected timeline point within summary series.  No guarantee that baseline data has the same timestamps as actual data (ZIP-684)
	const baselineSummaryPointIndex = $derived(
		columnSummaryBaselineData?.timestamps?.findIndex(
			(ts) => (ts as unknown as number) === baselinePointDate?.getTime()
		) ?? -1
	);

	// TODO: Demo purposes.  Remove later
	function remapHistogramKey(key: string) {
		return key.replace('BR', 'fr').replace('ET', 'au').replace('GE', 'de');
	}

	// TODO: Demo purposes.  Remove later
	function remapHistogramKeys(data: ITileSummarySeriesArgs) {
		let result = { ...data };
		// @ts-expect-error - Actually an Object and not a Map
		result.histogram = Object.fromEntries(
			Object.entries(data.histogram ?? {}).map(([key, value]) => {
				return [remapHistogramKey(key), value];
			})
		);
		return result;
	}

	function selectSeries(chartSeries: SeriesData<any, any>, _point: DateValue) {
		// Find matching `tileSeriesKey` for chart series
		tileSeriesKey = groupDriftSeries.find((ds) => ds.key?.column === chartSeries.key)?.key;
		point = _point;
		color = chartSeries.color ?? 'red';
	}
	let lockedTooltip = $state(false);
</script>

<Drawer
	open={tileSeriesKey != null}
	on:close
	let:close
	classes={{
		root: 'border-l'
	}}
>
	{#if tileSeriesKey && point}
		<div class="w-[1000px] max-w-[90vw] pb-10">
			<header class="sticky top-0 z-10 bg-surface-100 border-b py-4 px-6 mb-3">
				<span
					class="mb-4 ml-2 text-xl font-medium flex items-center gap-2 w-fit"
					role="presentation"
					onmouseenter={() => {
						/*highlightSeries(selectedSeries ?? '', dialogGroupChart, 'highlight')*/
					}}
					onmouseleave={() => {
						/*highlightSeries(selectedSeries ?? '', dialogGroupChart, 'downplay')*/
					}}
				>
					<div class="w-3 h-3 rounded-full" style:background-color={color}></div>
					<div>
						{tileSeriesKey.column}
					</div>
				</span>

				<div class="flex items-center gap-3">
					<DateRangeField label="Date range" value={dateRange} />

					<TextField
						label="Offset:"
						type="integer"
						bind:value={params.offset}
						dense
						classes={{
							root: '[--color-primary:var(--color-primary-800)] border focus-within:border-primary',
							container: 'bg-transparent border-0',
							input: 'w-6'
						}}
					>
						<div slot="suffix" class="pl-1 text-surface-content/50 text-sm">days</div>
					</TextField>

					<Field label="Baseline" classes={{ input: 'text-sm' }} dense>
						<div
							class="inline-flex items-center justify-center bg-white text-black rounded-full size-4 text-sm font-bold mr-2"
							slot="prepend"
						>
							B
						</div>
						{format(baselinePointDate, PeriodType.DayTime)}
					</Field>

					<IconChevronsLeftRightEllipsis />

					<Field label="Actual" classes={{ input: 'text-sm' }} dense>
						<div
							class="inline-flex items-center justify-center bg-white text-black rounded-full size-4 text-sm font-bold mr-2"
							slot="prepend"
						>
							A
						</div>
						{format(actualPointDate, PeriodType.DayTime)}
					</Field>

					{#if isZoomed()}
						<ResetZoomButton onClick={resetZoom} />
					{/if}
				</div>

				<Button class="absolute top-2 right-2 w-8 h-8 p-0" on:click={() => close()}>
					<IconXMark />
					<span class="sr-only">Close</span>
				</Button>
			</header>

			<div class="grid gap-6 px-6 overflow-y-auto">
				{#if columnSummaryActualResource.current}
					<CollapsibleSection open>
						{#snippet trigger()}
							Drift
							<div class="grow-1 flex justify-end gap-3">
								<span class="flex border rounded-md text-surface-content/50 text-sm">
									<span
										class="border-r px-1 bg-neutral-200 font-medium text-xs inline-block pt-[2px]"
									>
										baseline:
									</span>
									<span class="px-1">
										{format(baselinePointDrift ?? 0, 'decimal', {
											fractionDigits: 4
										})}
									</span>
								</span>
								<span class="flex border rounded-md text-surface-content/50 text-sm">
									<span
										class="border-r px-1 bg-neutral-200 font-medium text-xs inline-block pt-[2px]"
									>
										actual:
									</span>
									<span class="px-1">
										{format(actualPointDrift ?? 0, 'decimal', {
											fractionDigits: 4
										})}
									</span>
								</span>
							</div>
						{/snippet}

						<div class="h-[280px]">
							<CustomLineChart
								series={transformSeries(
									// groupDriftSeries,
									[driftSeries],
									(s) =>
										(s.percentileDriftSeries ?? s.histogramDriftSeries ?? []).map((value, i) => {
											// TODO: Demo purposes to hide data with "job running" window
											return i > 1813 ? (null as unknown as number) : value;
										})
								).map((s) => {
									return {
										...s,
										color: s.key === tileSeriesKey?.column ? color : 'hsl(0 0% 100% / 20%)'
									};
								})}
								annotations={[
									{
										type: 'line',
										x: actualPointDate,
										seriesKey: tileSeriesKey.column
									},
									{
										type: 'point',
										label: 'A',
										x: actualPointDate,
										y: actualPointDrift,
										seriesKey: tileSeriesKey.column
									},
									{
										type: 'line',
										x: baselinePointDate,
										seriesKey: tileSeriesKey.column
									},
									{
										type: 'point',
										label: 'B',
										x: baselinePointDate,
										y: baselinePointDrift,
										seriesKey: tileSeriesKey.column
									},
									// Demo purposes: Show "missing data" and alert
									{
										type: 'range',
										x: [parseISO('2023-02-07'), dateRange.end],
										pattern: {
											size: 8,
											lines: {
												rotate: -45,
												opacity: 0.2
											}
										}
									},
									{
										type: 'range',
										x: [parseISO('2023-01-27T19:00'), parseISO('2023-02-02T19:00')],
										seriesKey: 'dim_merchant_country',
										gradient: {
											stops: ['hsl(40 100% 25% / 50%)', 'transparent'],
											vertical: true
										},
										layer: 'below'
									},
									{
										type: 'point',
										label: '!',
										x: parseISO('2023-01-27T19:00'),
										seriesKey: 'dim_merchant_country',
										classes: {
											circle: 'fill-[hsl(40_100%_50%)]'
										}
									}
								]}
								yDomain={driftMetricDomain}
								xDomain={shared.xDomain}
								onBrushEnd={(detail: { xDomain?: DomainType }) => {
									shared.xDomain = detail.xDomain;
								}}
								onPointClick={(_e: MouseEvent, { series, data }) => {
									selectSeries(series, data as DateValue);
								}}
								onItemClick={({
									series,
									data
								}: {
									series: SeriesData<any, any>;
									data: DateValue;
								}) => {
									selectSeries(series, data as DateValue);
								}}
								{lockedTooltip}
								tooltipFormat={formatDrift}
							/>
						</div>
					</CollapsibleSection>
				{/if}

				{#if columnSummaryActualData?.percentiles}
					<CollapsibleSection label="Values over time">
						<div class="h-[280px]">
							<PercentileLineChart
								data={columnSummaryActualData}
								annotations={[
									{ type: 'line', x: actualPointDate },
									{
										type: 'line',
										x: baselinePointDate
									}
								]}
								xDomain={shared.xDomain}
								onBrushEnd={(detail: { xDomain?: DomainType }) => {
									shared.xDomain = detail.xDomain;
								}}
								{lockedTooltip}
							/>
						</div>
					</CollapsibleSection>
				{/if}

				{#if columnSummaryActualData && Object.keys(columnSummaryActualData?.histogram ?? {}).length > 0}
					<CollapsibleSection label="Values over time">
						<div class="h-[280px]">
							<HistogramLineChart
								data={remapHistogramKeys(columnSummaryActualData)}
								annotations={[
									{ type: 'line', x: actualPointDate },
									{
										type: 'point',
										label: 'A',
										x: actualPointDate
									},
									{
										type: 'line',
										x: baselinePointDate
									},
									{
										type: 'point',
										label: 'B',
										x: baselinePointDate
									}
								]}
								xDomain={shared.xDomain}
								onBrushEnd={(detail: { xDomain?: DomainType }) => {
									shared.xDomain = detail.xDomain;
								}}
								{lockedTooltip}
							/>
						</div>
					</CollapsibleSection>
				{/if}

				{#if columnSummaryActualData && Object.keys(columnSummaryActualData?.histogram ?? {}).length > 0}
					{@const actualData = entries(
						columnSummaryActualData.histogram as unknown as Record<string, number[]>
					).map(([key, values]) => {
						const value = values[actualSummaryPointIndex];
						return {
							label: remapHistogramKey(key),
							value: value === NULL_VALUE ? null : value
						};
					})}
					{@const baselineData = entries(
						(columnSummaryBaselineData?.histogram ?? {}) as unknown as Record<string, number[]>
					).map(([key, values]) => {
						const value = values[baselineSummaryPointIndex];
						return {
							label: remapHistogramKey(key),
							value: value === NULL_VALUE ? null : value
						};
					})}

					<Toggle let:on={showValues} let:toggle>
						<CollapsibleSection open>
							{#snippet trigger()}
								<div class="flex-1 flex items-center justify-between gap-3">
									<span>Data Distribution</span>
									<ToggleGroup
										value={showValues}
										on:change={toggle}
										rounded="full"
										variant="outline"
										inset
										classes={{
											root: '[--color-primary:var(--color-primary-800)]',
											options: 'justify-start'
										}}
									>
										<ToggleOption value={false}>Delta</ToggleOption>
										<ToggleOption value={true}>Values</ToggleOption>
									</ToggleGroup>
								</div>
							{/snippet}

							<div class="h-[260px]">
								{#if showValues}
									<BarChart
										x="label"
										y="value"
										series={[
											{
												key: 'Baseline',
												data: baselineData.sort(
													sortFunc((d) => {
														// Sort data to match delta chart order
														const actualValue =
															actualData.find((a) => a.label === d.label)?.value ?? 0;
														return actualValue - (d.value ?? 0);
													})
												),
												color: '#2976E6' // TODO: copied from ECharts defaults
											},
											{
												key: 'Actual',
												data: actualData.sort(
													sortFunc((d) => {
														// Sort data to match delta chart order
														const baselineValue =
															baselineData.find((a) => a.label === d.label)?.value ?? 0;
														return (d.value ?? 0) - baselineValue;
													})
												),
												color: '#3DDC91' // TODO: copied from ECharts defaults
											}
										]}
										seriesLayout="group"
										groupPadding={0.1}
										bandPadding={0.1}
										{...barChartProps}
										props={{
											yAxis: { ...yAxisProps },
											xAxis: { ...xAxisProps },
											tooltip: { ...tooltipProps, hideTotal: true },
											bars: {
												strokeWidth: 0
											}
										}}
										legend
									/>
								{:else}
									{@const chartData = actualData
										.map((d, i) => {
											const baselineValue = baselineData.find((a) => a.label === d.label)?.value;
											return {
												label: d.label,
												value: (d.value ?? 0) - (baselineValue ?? 0)
											};
										})
										.sort(sortFunc('value'))}
									<BarChart
										data={chartData}
										x="label"
										y="value"
										c="value"
										cScale={scaleThreshold()}
										cDomain={[0]}
										cRange={[successColor, dangerColor]}
										bandPadding={0.1}
										{...barChartProps}
										props={{
											yAxis: { ...yAxisProps },
											xAxis: { ...xAxisProps },
											tooltip: { ...tooltipProps, hideTotal: true },
											bars: {
												strokeWidth: 0
											}
										}}
									/>
								{/if}
							</div>
						</CollapsibleSection>
					</Toggle>
				{/if}

				{#if columnSummaryActualData?.count && columnSummaryActualData?.nullCount && columnSummaryBaselineData?.count && columnSummaryBaselineData?.nullCount}
					<!-- Use `Number()` to fix types -->
					<!-- TODO: Remove random value once data is populated -->
					{@const charts = [
						{
							label: 'Baseline',
							nullRatio:
								// Number(columnSummaryBaselineData.nullCount[baselineSummaryPointIndex]) /
								// Number(columnSummaryBaselineData.count[baselineSummaryPointIndex])
								randomUniform(0.008, 0.009)()
						},
						{
							label: 'Current',
							nullRatio:
								// Number(columnSummaryActualData.nullCount[actualSummaryPointIndex]) /
								// Number(columnSummaryActualData.count[actualSummaryPointIndex])
								randomUniform(0.008, 0.009)()
						}
					]}

					<CollapsibleSection label="Null Ratio" open>
						<div class="grid grid-cols-2 gap-3">
							{#each charts as chart}
								<div class="grid grid-stack items-center">
									<div class="h-[240px]">
										<PieChart
											data={[
												{
													label: 'Null %',
													value: chart.nullRatio
												},
												{
													label: 'Non-null %',
													value: 1 - chart.nullRatio
												}
											]}
											innerRadius={0.75}
											key="label"
											value="value"
											cRange={[successColor, dangerColor]}
											cornerRadius={8}
											padAngle={0.02}
											{...pieChartProps}
										/>
									</div>
									<div class="text-center">
										<div class="text-lg font-medium text-neutral-900">{chart.label}</div>
										<div class="text-neutral-700">{format(chart.nullRatio, 'percent')}</div>
									</div>
								</div>
							{/each}
						</div>
					</CollapsibleSection>
				{/if}
			</div>
		</div>
	{/if}
</Drawer>

<svelte:window
	onkeydown={(e) => {
		if (isMacOS() ? e.metaKey : e.ctrlKey) {
			lockedTooltip = true;
		}
	}}
	onkeyup={(e) => {
		if (isMacOS() ? !e.metaKey : !e.ctrlKey) {
			lockedTooltip = false;
		}
	}}
/>

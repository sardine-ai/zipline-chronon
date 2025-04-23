<script lang="ts">
	import { type DomainType } from 'layerchart/utils/scales.svelte';
	import { rollups } from 'd3';
	import { sort } from '@layerstack/utils';
	import { Lazy, Toggle, ToggleGroup, ToggleOption } from 'svelte-ux';
	import { type SeriesData } from 'layerchart';

	import CustomLineChart from '$src/lib/components/charts/CustomLineChart.svelte';
	import CollapsibleSection from '$src/lib/components/CollapsibleSection.svelte';

	import { isMacOS } from '$src/lib/util/browser';
	import { colors, type DateValue } from '$src/lib/components/charts/common';
	import { transformSeries } from '$src/lib/components/charts/utils';
	import { NULL_VALUE } from '$src/lib/constants/common';

	import { shared } from '../../../shared.svelte';

	import IconHash from '~icons/lucide/hash';
	import IconPercent from '~icons/lucide/percent';
	import { DRIFT_METRIC_SCALES } from '$src/lib/params/drift-metric';
	import DriftDrilldownDrawer from './DriftDrilldownDrawer.svelte';
	import type { ITileDriftSeriesArgs, ITileSeriesKeyArgs } from '$src/lib/types/codegen';
	import { formatDrift } from '$src/lib/util/format';

	let { data } = $props();

	// Group by group name and sort groups and columns
	const driftSeriesByGroupName = $derived(
		sort(
			rollups(
				data.joinDrift.driftSeries ?? [],
				(values) => sort(values, (d) => d.key.column, 'asc'),
				(d) => d.key?.groupName ?? 'Unknown'
			),
			(d) => d[0],
			'asc' // TODO: allow changing sort direction?
		)
	);

	const driftMetricDomain = $derived.by(() => {
		const scale = DRIFT_METRIC_SCALES[data.driftMetric];
		return [scale.min, scale.max];
	});

	let selectedSeries = $state<{
		tileSeriesKey: ITileSeriesKeyArgs;
		point: DateValue;
		color: string;
		groupDriftSeries: ITileDriftSeriesArgs[];
	} | null>(null);

	function selectSeries(
		chartSeries: SeriesData<any, any>,
		point: DateValue,
		groupDriftSeries: ITileDriftSeriesArgs[]
	) {
		const tileSeriesKey = groupDriftSeries.find((ds) => ds.key?.column === chartSeries.key)?.key;

		selectedSeries = {
			tileSeriesKey: tileSeriesKey!,
			point,
			color: chartSeries.color ?? 'red',
			groupDriftSeries
		};
	}

	let lockedTooltip = $state(false);
</script>

<div class="border rounded-md mt-4 divide-y px-2">
	<CollapsibleSection label="Row count" open class="py-4 text-sm">
		<!-- Use the first series for overall row count values -->
		{@const values = data.rowCountDriftSeries}
		<div class="h-[280px]">
			<CustomLineChart
				series={[
					{
						key: 'total',

						data: values?.timestamps?.map((ts, i) => {
							const count = values.countChangePercentSeries![i] as number;
							const nullCount = values.nullRatioChangePercentSeries![i] as number;
							return {
								date: new Date(ts as number),
								value:
									count == null ||
									count === NULL_VALUE ||
									nullCount == null ||
									nullCount === NULL_VALUE
										? null
										: count + nullCount
							};
						}),
						color: colors[0]
					}
				]}
				yDomain={driftMetricDomain}
				xDomain={shared.xDomain}
				onBrushEnd={(detail: { xDomain?: DomainType }) => {
					shared.xDomain = detail.xDomain;
				}}
				{lockedTooltip}
				annotations={data.annotations.filter((a) => a.seriesKey == null)}
				legend={false}
				padding={{ top: 8, left: 36, bottom: 24 }}
				tooltipFormat={formatDrift}
			/>
		</div>
	</CollapsibleSection>
</div>

<div class="border rounded-md mt-4 divide-y px-2">
	{#each driftSeriesByGroupName as [groupName, groupDriftSeries] (groupName)}
		<Toggle let:on={showCounts} let:toggle>
			<CollapsibleSection label={groupName} open class="py-4 text-sm">
				<div class="grid gap-2">
					<div
						class="grid grid-cols-[auto_1fr] items-center gap-1 mx-4 bg-neutral-200 p-4 rounded-md"
					>
						<div class="[writing-mode:vertical-lr] rotate-180 text-surface-content/50">Values</div>
						<Lazy height={280} unmount>
							<div class="h-[280px]">
								<CustomLineChart
									series={transformSeries(groupDriftSeries, (s) =>
										(s.percentileDriftSeries ?? s.histogramDriftSeries ?? []).map((value, i) => {
											// TODO: Demo purposes to hide data with "job running" window
											return i > 1813 ? (null as unknown as number) : value;
										})
									)}
									yDomain={driftMetricDomain}
									onPointClick={(_, { series, data }) => {
										selectSeries(series, data as DateValue, groupDriftSeries);
									}}
									onItemClick={({ series, data }) => {
										selectSeries(series, data, groupDriftSeries);
									}}
									xDomain={shared.xDomain}
									onBrushEnd={(detail: { xDomain?: DomainType }) => {
										shared.xDomain = detail.xDomain;
									}}
									{lockedTooltip}
									thresholds={[
										// {
										// 	label: '90%',
										// 	value: (driftSeries[0].percentileDriftSeries![0] as number) * 0.9
										// }
									]}
									annotations={data.annotations.filter(
										(a) => a.seriesKey != null || (a.type === 'range' && a.pattern)
									)}
									tooltipFormat={formatDrift}
								/>
							</div>
						</Lazy>
					</div>

					<div
						class="grid grid-cols-[auto_1fr] items-center gap-1 mx-4 bg-neutral-200 p-4 rounded-md"
					>
						<div class="[writing-mode:vertical-lr] rotate-180 text-surface-content/50">
							Null ratio
						</div>

						<div>
							<div class="flex justify-end">
								<ToggleGroup
									value={showCounts}
									on:change={toggle}
									rounded="full"
									variant="outline"
									inset
									classes={{
										root: '[--color-primary:var(--color-primary-800)]',
										options: 'justify-start'
									}}
								>
									<ToggleOption value={false}><IconPercent /></ToggleOption>
									<ToggleOption value={true}><IconHash /></ToggleOption>
								</ToggleGroup>
							</div>

							<Lazy height={280} unmount>
								<div class="h-[280px]">
									<CustomLineChart
										series={showCounts
											? [
													{
														key: 'row count',

														data: groupDriftSeries[0]?.timestamps?.map((ts, i) => {
															const count = groupDriftSeries[0].countChangePercentSeries![
																i
															] as number;
															const nullCount = groupDriftSeries[0].nullRatioChangePercentSeries![
																i
															] as number;
															return {
																date: new Date(ts as number),
																value:
																	count == null ||
																	count === NULL_VALUE ||
																	nullCount == null ||
																	nullCount === NULL_VALUE
																		? null
																		: count + nullCount
															};
														}),
														color: 'var(--color-neutral-800)'
													},
													...transformSeries(
														groupDriftSeries,
														(s) => s.nullRatioChangePercentSeries as number[]
													)
												]
											: transformSeries(
													groupDriftSeries,
													(s) =>
														s.countChangePercentSeries?.map((c, i) => {
															const count = c as number;
															const nullCount =
																(s.nullRatioChangePercentSeries?.[i] as number) ?? 0;
															const total = count + nullCount;
															// If total is 0, show as 100% null
															return total === 0 ? 1 : nullCount / total;
														}) ?? []
												)}
										format={showCounts ? 'metric' : 'percent'}
										yDomain={showCounts ? null : [0, 1]}
										onPointClick={(_, { series, data }) => {
											selectSeries(series, data as DateValue, groupDriftSeries);
										}}
										onItemClick={({ series, data }) => {
											selectSeries(series, data as DateValue, groupDriftSeries);
										}}
										xDomain={shared.xDomain}
										onBrushEnd={(detail: { xDomain?: DomainType }) => {
											shared.xDomain = detail.xDomain;
										}}
										{lockedTooltip}
										annotations={data.annotations.filter(
											(a) => /*a.seriesKey != null ||*/ a.type === 'range' && a.pattern
										)}
									/>
								</div>
							</Lazy>
						</div>
					</div>
				</div>
			</CollapsibleSection>
		</Toggle>
	{/each}
</div>

<!-- Add extra space at bottom of page for tooltip -->
<div class="h-[300px]"></div>

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

<DriftDrilldownDrawer
	tileSeriesKey={selectedSeries?.tileSeriesKey}
	{driftMetricDomain}
	dateRange={data.dateRange}
	point={selectedSeries?.point}
	color={selectedSeries?.color ?? 'red'}
	groupDriftSeries={driftSeriesByGroupName.find(
		(d) => d[0] === selectedSeries?.tileSeriesKey.groupName
	)?.[1] ?? []}
	on:close={() => {
		selectedSeries = null;
	}}
/>

<script lang="ts">
	import { type DomainType } from 'layerchart/utils/scales.svelte';
	import { rollups } from 'd3';
	import { sort } from '@layerstack/utils';
	import { Lazy, Toggle, ToggleGroup, ToggleOption } from 'svelte-ux';

	import CustomLineChart from '$src/lib/components/charts/CustomLineChart.svelte';
	import CollapsibleSection from '$src/lib/components/CollapsibleSection.svelte';

	import { isMacOS } from '$src/lib/util/browser';
	import { colors, type DateValue } from '$src/lib/components/charts/common';
	import { transformSeries } from '$src/lib/components/charts/utils';
	import { NULL_VALUE } from '$src/lib/constants/common';

	import { shared } from '../../../shared.svelte';
	import HistogramLineChart from '$src/lib/components/charts/HistogramLineChart.svelte';
	import PercentileLineChart from '$src/lib/components/charts/PercentileLineChart.svelte';

	import IconHash from '~icons/lucide/hash';
	import IconPercent from '~icons/lucide/percent';

	let { data } = $props();

	// Group by group name and sort groups and columns
	const columnSummariesByGroupName = $derived(
		sort(
			rollups(
				data.columnSummaries ?? [],
				(values) => sort(values, (d) => d.key.column, 'asc'),
				(d) => d.key?.groupName ?? 'Unknown'
			),
			(d) => d[0],
			'asc' // TODO: allow changing sort direction?
		)
	);

	let lockedTooltip = $state(false);
</script>

<div class="border rounded-md mt-4 divide-y px-2">
	<CollapsibleSection label="Row count" open class="py-4 text-sm">
		<!-- Use the first series for overall row count values -->
		{@const values = data.rowCountSummarySeries}
		<div class="h-[280px]">
			<CustomLineChart
				series={[
					{
						key: 'total',

						data: values?.timestamps?.map((ts, i) => {
							const count = values.count![i] as number;
							const nullCount = values.nullCount![i] as number;
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
				format="metric"
				xDomain={shared.xDomain}
				onBrushEnd={(detail: { xDomain?: DomainType }) => {
					shared.xDomain = detail.xDomain;
				}}
				{lockedTooltip}
				thresholds={[
					//{ label: '90%', value: (values.count![0] as number) * 0.9 }
				]}
				annotations={data.annotations.filter((a) => a.seriesKey == null)}
				legend={false}
				padding={{ top: 8, left: 36, bottom: 24 }}
			/>
		</div>
	</CollapsibleSection>
</div>

<div class="border rounded-md mt-4 divide-y px-2">
	{#each columnSummariesByGroupName as [groupName, columnSummaries] (groupName)}
		<Toggle let:on={showCounts} let:toggle>
			<CollapsibleSection label={groupName} open class="py-4 text-sm">
				<div class="grid grid-cols-[auto_1fr] items-center gap-1 mx-4">
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

													data: columnSummaries[0]?.timestamps?.map((ts, i) => {
														const count = columnSummaries[0].count![i] as number;
														const nullCount = columnSummaries[0].nullCount![i] as number;
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
												...transformSeries(columnSummaries, (s) => s.nullCount as number[])
											]
										: transformSeries(
												columnSummaries,
												(s) =>
													s.count?.map((c, i) => {
														const count = c as number;
														const nullCount = (s.nullCount?.[i] as number) ?? 0;
														const total = count + nullCount;
														// If total is 0, show as 100% null
														return total === 0 ? 1 : nullCount / total;
													}) ?? []
											)}
									yDomain={showCounts ? null : [0, 1]}
									xDomain={shared.xDomain}
									onBrushEnd={(detail: { xDomain?: DomainType }) => {
										shared.xDomain = detail.xDomain;
									}}
									format={showCounts ? 'metric' : 'percent'}
									{lockedTooltip}
									thresholds={showCounts
										? []
										: [
												{
													label: '90%',
													value: 0.9
												}
											]}
									annotations={data.annotations.filter(
										(a) => a.seriesKey != null || (a.type === 'range' && a.pattern)
									)}
								/>
							</div>
						</Lazy>
					</div>
				</div>

				<div class="border rounded-md mt-4 mx-4 mb-4 px-4 bg-neutral-500/10 divide-y">
					<CollapsibleSection label="Feature summaries" class="py-3">
						<div class="pt-1">
							<div class="divide-y divide-neutral-200 bg-neutral-300 rounded-md">
								{#each columnSummaries as columnSummary (columnSummary.key?.column)}
									<CollapsibleSection
										label={columnSummary.key?.column ?? 'Unknown'}
										class="pl-4 not-last:pb-3 py-3 text-sm"
									>
										<Lazy height={230} unmount>
											<div class="h-[230px]">
												{#if columnSummary.percentiles}
													<PercentileLineChart
														data={columnSummary}
														xDomain={shared.xDomain}
														onBrushEnd={(detail: { xDomain?: DomainType }) => {
															shared.xDomain = detail.xDomain;
														}}
														renderContext="canvas"
													/>
												{:else if columnSummary.histogram}
													<HistogramLineChart
														data={columnSummary}
														xDomain={shared.xDomain}
														onBrushEnd={(detail: { xDomain?: DomainType }) => {
															shared.xDomain = detail.xDomain;
														}}
														renderContext="canvas"
													/>
												{:else}
													<div class="bg-neutral-500/10 p-4 rounded-md h-full">
														Unknown column type
													</div>
												{/if}
											</div>
										</Lazy>
									</CollapsibleSection>
								{/each}
							</div>
						</div>
					</CollapsibleSection>
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

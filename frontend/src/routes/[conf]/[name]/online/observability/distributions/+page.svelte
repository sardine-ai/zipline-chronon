<script lang="ts">
	import type { DomainType } from 'layerchart/utils/scales.svelte';
	import { sort } from '@layerstack/utils';
	import { rollups } from 'd3';
	import { Collapse, Lazy } from 'svelte-ux';

	import { page } from '$app/state';

	import CollapsibleSection from '$src/lib/components/CollapsibleSection.svelte';
	import { getSortDirection } from '$lib/params/sort';
	import PercentileLineChart from '$src/lib/components/charts/PercentileLineChart.svelte';
	import CustomLineChart from '$src/lib/components/charts/CustomLineChart.svelte';
	import { isMacOS } from '$src/lib/util/browser.js';
	import { generateTileSummarySeriesData } from '$src/lib/util/test-data/tile-series.js';

	import { transformSeries } from '$src/lib/components/charts/utils';
	import { shared } from '../../../shared.svelte.js';

	const { data } = $props();

	const sortDirection = $derived(getSortDirection(page.url.searchParams, 'summary'));

	// Group by group name and sort groups and columns
	const columnSummariesByGroupName = $derived(
		sort(
			rollups(
				data.columnSummaries,
				(values) => sort(values, (d) => d.key.column, 'asc'),
				(d) => d.key?.groupName ?? 'Unknown'
			),
			(d) => d[0],
			sortDirection
		)
	);

	let lockedTooltip = $state(false);

	const testDataConfigs = [
		{
			key: {
				column: 'txn_by_merchant_transaction_amount_count_7d'
			},
			normal: { total: [70, 80], nullRatio: [0.5, 0.6] },
			anomalies: []
		},
		{
			key: {
				column: 'txn_by_merchant_transaction_amount_count_1d'
			},
			normal: { total: [1000, 1200], nullRatio: [0.1, 0.2] },
			anomalies: []
		},
		{
			key: {
				column: 'dim_merchant_zipcode'
			},
			normal: { total: [41561, 41561], nullRatio: [0.1, 0.2] },
			anomalies: []
		}
	];

	const testData = testDataConfigs.map((config) =>
		generateTileSummarySeriesData({
			...config,
			startDate: new Date(data.dateRange.startTimestamp),
			endDate: new Date(data.dateRange.endTimestamp),
			points: data.columnSummaries[0].timestamps?.length ?? 500
		})
	);
</script>

{#if columnSummariesByGroupName.length === 0}
	<div class="mt-6 bg-destructive/10 border border-destructive/50 p-4 rounded font-medium">
		Column summary unavailable
	</div>
{:else}
	<div class="grid gap-4 mt-4">
		<div class="border rounded-md divide-y">
			<CollapsibleSection label="Null rate" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(
							testData,
							(s) =>
								s.count?.map((c, i) => {
									const count = c as number;
									const nullCount = (s.nullCount?.[i] as number) ?? 0;
									return nullCount / (count + nullCount);
									// return nullCount / count;
								}) ?? []
						)}
						xDomain={shared.xDomain}
						yDomain={[0, 1]}
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							shared.xDomain = detail.xDomain;
						}}
						format="percent"
						{lockedTooltip}
					/>
				</div>
			</CollapsibleSection>

			<CollapsibleSection label="Row count" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(
							testData,
							(s) =>
								s.count?.map((c, i) => {
									const count = c as number;
									const nullCount = (s.nullCount?.[i] as number) ?? 0;
									return count + nullCount;
								}) ?? []
						)}
						xDomain={shared.xDomain}
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							shared.xDomain = detail.xDomain;
						}}
						format="metric"
						{lockedTooltip}
					/>
				</div>
			</CollapsibleSection>

			<!-- <CollapsibleSection label="Null rate" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(
							data.columnSummaries,
							(s) =>
								s.count?.map((c, i) => {
									const count = c as number;
									const nullCount = (s.nullCount?.[i] as number) ?? 0;
									// return nullCount;
									return nullCount / (count + nullCount);
								}) ?? []
						)}
						xDomain={shared.xDomain}
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							shared.xDomain = detail.xDomain;
						}}
						format="percent"
						{lockedTooltip}
					/>
				</div>
			</CollapsibleSection>

			<CollapsibleSection label="Row count" open class="p-4 text-sm">
				<div class="h-[280px]">
					<CustomLineChart
						series={transformSeries(
							data.columnSummaries,
							(s) =>
								s.count?.map((c) => {
									const count = c as number;
									// const nullCount = (s.nullCount?.[i] as number) ?? 0;
									// return count + nullCount;
									return count;
								}) ?? []
						)}
						xDomain={shared.xDomain}
						onBrushEnd={(detail: { xDomain?: DomainType }) => {
							shared.xDomain = detail.xDomain;
						}}
						{lockedTooltip}
					/>
				</div>
			</CollapsibleSection> -->
		</div>

		<div class="border rounded-md divide-y">
			{#each columnSummariesByGroupName as [groupName, columnSummaries] (groupName)}
				<Collapse
					name={groupName}
					open
					classes={{
						root: '[&>button]:gap-3',
						trigger: 'text-sm py-4',
						icon: 'order-first text-sm ml-2 data-[open=true]:rotate-0 data-[open=false]:-rotate-90'
					}}
				>
					<div class="border rounded-md mx-4 mb-4 px-4 bg-neutral-500/10 divide-y">
						{#each columnSummaries as columnSummary (columnSummary.key?.column)}
							<CollapsibleSection
								label={columnSummary.key?.column ?? 'Unknown'}
								open
								class="not-last:pb-6 py-6 text-sm"
							>
								<Lazy height={230}>
									<div class="h-[230px] mt-4">
										<PercentileLineChart
											data={columnSummary}
											xDomain={shared.xDomain}
											onBrushEnd={(detail: { xDomain?: DomainType }) => {
												shared.xDomain = detail.xDomain;
											}}
											renderContext="canvas"
										/>
									</div>
								</Lazy>
							</CollapsibleSection>
						{/each}
					</div>
				</Collapse>
			{/each}
		</div>
	</div>
{/if}

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

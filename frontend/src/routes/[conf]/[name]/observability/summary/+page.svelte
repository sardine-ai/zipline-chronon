<script lang="ts">
	import type { DomainType } from 'layerchart/utils/scales';
	import { sort } from '@layerstack/utils';
	import { rollups } from 'd3';
	import { Collapse } from 'svelte-ux';

	import { page } from '$app/state';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import { getSortDirection } from '$lib/util/sort';
	import ChartControls from '$src/lib/components/ChartControls.svelte';
	import ObservabilityNavTabs from '$routes/[conf]/[name]/observability/ObservabilityNavTabs.svelte';
	import { Separator } from '$src/lib/components/ui/separator';
	import PercentileLineChart from '$src/lib/components/charts/PercentileLineChart.svelte';
	import IconChevronDown from '~icons/heroicons/chevron-down-16-solid';

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

	let xDomain = $state<DomainType | undefined>(null);
	let isZoomed = $derived(xDomain != null);

	function resetZoom() {
		xDomain = null;
	}
</script>

<div
	class="sticky top-0 z-20 bg-neutral-50 dark:bg-neutral-100 border-b border-border -mx-8 py-2 px-8 border-l"
>
	<ChartControls
		{isZoomed}
		onResetZoom={resetZoom}
		isUsingFallbackDates={data.dateRange.isUsingFallback}
		dateRange={{
			startTimestamp: data.dateRange.startTimestamp,
			endTimestamp: data.dateRange.endTimestamp
		}}
		showSort
		context="summary"
	/>
</div>

<Separator fullWidthExtend={true} wide={true} />
<ObservabilityNavTabs />

<div class="border rounded-md mt-4 divide-y">
	{#if columnSummariesByGroupName.length === 0}
		<div class="mt-6 bg-destructive/10 border border-destructive/50 p-4 rounded font-medium">
			No column summary data available
		</div>
	{:else}
		{#each columnSummariesByGroupName as [groupName, columnSummaries] (groupName)}
			<Collapse
				name={groupName}
				open
				classes={{
					root: '[&>button]:flex-row-reverse [&>button]:gap-4',
					trigger: 'text-regular py-4'
				}}
			>
				<svelte:fragment slot="icon" let:open>
					<IconChevronDown
						class="size-4 ml-2 transition-transform duration-200 {open ? '' : '-rotate-90'}"
					/>
				</svelte:fragment>

				<div class="border rounded-md mx-4 mb-4 px-4 bg-neutral-500/10 divide-y">
					{#each columnSummaries as columnSummary (columnSummary.key?.column)}
						<CollapsibleSection
							title={columnSummary.key?.column ?? 'Unknown'}
							size="small"
							open
							class="not-last:pb-6"
						>
							{#snippet collapsibleContent()}
								<div class="h-[230px]">
									<PercentileLineChart
										data={columnSummary}
										{xDomain}
										onbrushend={(detail: { xDomain?: DomainType }) => {
											xDomain = detail.xDomain;
										}}
										renderContext="canvas"
									/>
								</div>
							{/snippet}
						</CollapsibleSection>
					{/each}
				</div>
			</Collapse>
		{/each}
	{/if}
</div>

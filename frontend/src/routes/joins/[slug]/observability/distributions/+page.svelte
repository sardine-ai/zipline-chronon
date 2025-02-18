<script lang="ts">
	import { queryParameters } from 'sveltekit-search-params';
	import type { DomainType } from 'layerchart/utils/scales';
	import { sort } from '@layerstack/utils';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import { getSortParamsConfig, getSortParamKey } from '$lib/util/sort';
	import type { ITileSummarySeries } from '$src/lib/types/codegen';
	import ChartControls from '$src/lib/components/ChartControls.svelte';
	import ObservabilityNavTabs from '$routes/joins/[slug]/observability/ObservabilityNavTabs.svelte';
	import { Separator } from '$src/lib/components/ui/separator';
	import ModelTable from '../ModelTable.svelte';
	import PercentileLineChart from '$src/lib/components/charts/PercentileLineChart.svelte';

	const { data } = $props();

	let isFeatureMonitoringOpen = $state(true);

	let isLoadingDistributions = $state(true);
	let distributions: ITileSummarySeries[] = $state([]);
	try {
		data.distributionsPromise.then((d) => {
			distributions = d;
			isLoadingDistributions = false;
		});
	} catch (err) {
		console.error('Error loading distributions:', err);
	}

	const sortContext = 'distributions';
	const sortKey = getSortParamKey(sortContext);
	const params = queryParameters(getSortParamsConfig(sortContext), {
		pushHistory: false,
		showDefaults: false
	});
	const sortedDistributions = $derived(sort(distributions, (d) => d.key?.column, params[sortKey]));

	let xDomain = $state<DomainType | undefined>(null);
	let isZoomed = $derived(xDomain != null);

	function resetZoom() {
		xDomain = null;
	}
</script>

{#if data.model}
	<ModelTable model={data.model} />
{/if}

<div class="sticky top-0 z-20 bg-neutral-100 border-b border-border -mx-8 py-2 px-8 border-l">
	<ChartControls
		{isZoomed}
		onResetZoom={resetZoom}
		isUsingFallbackDates={data.dateRange.isUsingFallback}
		dateRange={{
			startTimestamp: data.dateRange.startTimestamp,
			endTimestamp: data.dateRange.endTimestamp
		}}
		showActionButtons={true}
		showSort={true}
		context="distributions"
	/>
</div>

<Separator fullWidthExtend={true} wide={true} />
<CollapsibleSection title="Feature Monitoring" bind:open={isFeatureMonitoringOpen}>
	{#snippet collapsibleContent()}
		<ObservabilityNavTabs />

		{#if isLoadingDistributions}
			<div class="mt-6">Loading distributions...</div>
		{:else if distributions.length === 0}
			<div class="mt-6 bg-destructive/10 border border-destructive/50 p-4 rounded font-medium">
				No distribution data available
			</div>
		{:else}
			{#each sortedDistributions as distribution}
				<CollapsibleSection title={distribution.key?.column ?? 'Unknown'} size="small" open={true}>
					{#snippet collapsibleContent()}
						<div class="h-[230px]">
							<PercentileLineChart
								data={distribution}
								{xDomain}
								onbrushend={(e) => (xDomain = e.xDomain)}
								renderContext="canvas"
							/>
						</div>
					{/snippet}
				</CollapsibleSection>
			{/each}
		{/if}
	{/snippet}
</CollapsibleSection>

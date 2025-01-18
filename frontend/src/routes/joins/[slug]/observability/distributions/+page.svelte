<script lang="ts">
	import { connect, type EChartsType, type EChartOption } from 'echarts';
	import { queryParameters } from 'sveltekit-search-params';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import PercentileChart from '$lib/components/PercentileChart.svelte';
	import { getSortParamsConfig, getSortParamKey, sortDistributions } from '$lib/util/sort';
	import type { FeatureResponse } from '$src/lib/types/Model/Model.js';
	import ChartControls from '$src/lib/components/ChartControls.svelte';
	import ObservabilityNavTabs from '../ObservabilityNavTabs.svelte';
	import { Separator } from '$src/lib/components/ui/separator';
	import ModelTable from '../ModelTable.svelte';
	import { untrack } from 'svelte';

	const { data } = $props();

	let isFeatureMonitoringOpen = $state(true);

	let distributionCharts: { [key: string]: EChartsType } = $state({});

	let isLoadingDistributions = $state(true);
	let distributions: FeatureResponse[] = $state([]);
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
	const sortedDistributions = $derived(sortDistributions(distributions, params[sortKey]));

	$effect(() => {
		connectCharts();
	});

	function connectCharts() {
		const allCharts = Object.values(distributionCharts);
		connect(allCharts);
	}

	let isZoomed = $state(false);
	let currentZoomState = $state({ start: 0, end: 100 });

	function handleZoom(event: CustomEvent<EChartOption.DataZoom>) {
		const detail = event.detail as {
			start?: number;
			end?: number;
			batch?: Array<{ startValue: number; endValue: number }>;
		};
		const start = detail.start ?? detail.batch?.[0]?.startValue ?? 0;
		const end = detail.end ?? detail.batch?.[0]?.endValue ?? 100;
		isZoomed = start !== 0 || end !== 100;
		currentZoomState = { start, end };
	}

	function resetZoom() {
		Object.values(distributionCharts).forEach((chart) => {
			chart.dispatchAction({ type: 'dataZoom', start: 0, end: 100 });
		});

		currentZoomState = { start: 0, end: 100 };
		isZoomed = false;
	}

	$effect(() => {
		const allCharts = Object.values(distributionCharts);
		if (allCharts.length) {
			untrack(() => {
				if (currentZoomState.start !== 0 && currentZoomState.end !== 100) {
					allCharts.forEach((chart) => {
						chart.dispatchAction({
							type: 'dataZoom',
							batch: [{ startValue: currentZoomState.start, endValue: currentZoomState.end }]
						});
					});
				}
			});
		}
	});
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
			<div class="mt-6">No distribution data available</div>
		{:else}
			{#each sortedDistributions as feature}
				<CollapsibleSection title={feature.feature} size="small" open={true}>
					{#snippet collapsibleContent()}
						<PercentileChart
							data={feature.current ?? null}
							bind:chartInstance={distributionCharts[feature.feature]}
							on:datazoom={handleZoom}
						/>
					{/snippet}
				</CollapsibleSection>
			{/each}
		{/if}
	{/snippet}
</CollapsibleSection>

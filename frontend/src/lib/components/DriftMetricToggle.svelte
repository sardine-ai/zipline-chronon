<script lang="ts">
	import { queryParameters } from 'sveltekit-search-params';

	import { Button } from '$lib/components/ui/button';
	import { DRIFT_METRIC_LABELS, getDriftMetricParamsConfig } from '$lib/util/drift-metric';
	import { enumValues, sortFunc } from '@layerstack/utils';
	import { DriftMetric } from '$lib/types/codegen';

	const paramConfig = getDriftMetricParamsConfig();
	const params = queryParameters(paramConfig, {
		pushHistory: false,
		showDefaults: false
	});

	// Sort default (PSI) before the other metrics
	const metrics = enumValues(DriftMetric).sort(
		sortFunc((metric) => (metric === paramConfig.metric.defaultValue ? -1 : 1))
	);
</script>

<div class="flex space-x-[1px]">
	{#each metrics as value}
		<Button
			variant={params.metric === value ? 'default' : 'secondary'}
			size="sm"
			on:click={() => (params.metric = value)}
			class="first:rounded-r-none last:rounded-l-none [&:not(:first-child):not(:last-child)]:rounded-none"
		>
			{DRIFT_METRIC_LABELS[value as DriftMetric]}
		</Button>
	{/each}
</div>

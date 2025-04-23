<script lang="ts">
	import { ToggleGroup, ToggleOption } from 'svelte-ux';
	import { queryParameters } from 'sveltekit-search-params';

	import { DRIFT_METRIC_LABELS, getDriftMetricParamsConfig } from '$lib/params/drift-metric';
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

<ToggleGroup
	value={params.metric}
	on:change={(e) => (params.metric = e.detail.value)}
	variant="default"
	gap="px"
	classes={{
		options: 'h-8',
		label:
			'bg-secondary text-secondary-foreground [&.selected]:text-primary-foreground hover:text-secondary-foreground hover:bg-secondary/80',
		indicator: 'bg-primary'
	}}
>
	{#each metrics as value}
		<ToggleOption {value}>
			{DRIFT_METRIC_LABELS[value as DriftMetric]}
		</ToggleOption>
	{/each}
</ToggleGroup>

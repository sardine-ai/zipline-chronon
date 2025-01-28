<script lang="ts">
	import { queryParameters } from 'sveltekit-search-params';

	import { Button } from '$lib/components/ui/button';
	import {
		METRIC_LABELS,
		METRIC_TYPES,
		getMetricTypeParamsConfig
	} from '$lib/types/MetricType/MetricType';

	const params = queryParameters(getMetricTypeParamsConfig(), {
		pushHistory: false,
		showDefaults: false
	});
</script>

<div class="flex space-x-[1px]">
	{#each METRIC_TYPES as metricType}
		<Button
			variant={params.metric === metricType ? 'default' : 'secondary'}
			size="sm"
			on:click={() => (params.metric = metricType)}
			class="first:rounded-r-none last:rounded-l-none [&:not(:first-child):not(:last-child)]:rounded-none"
		>
			{METRIC_LABELS[metricType]}
		</Button>
	{/each}
</div>

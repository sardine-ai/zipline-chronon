<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import {
		METRIC_LABELS,
		METRIC_TYPES,
		type MetricType,
		getMetricTypeFromParams
	} from '$lib/types/MetricType/MetricType';
	import { page } from '$app/stores';
	import { goto } from '$app/navigation';

	let selected = $derived(getMetricTypeFromParams(new URL($page.url).searchParams));

	function toggle(value: MetricType) {
		const url = new URL($page.url);
		url.searchParams.set('metric', value);
		goto(url, { replaceState: true });
	}
</script>

<div class="flex space-x-[1px]">
	{#each METRIC_TYPES as metricType}
		<Button
			variant={selected === metricType ? 'default' : 'secondary'}
			size="sm"
			on:click={() => toggle(metricType)}
			class="first:rounded-r-none last:rounded-l-none [&:not(:first-child):not(:last-child)]:rounded-none"
		>
			{METRIC_LABELS[metricType]}
		</Button>
	{/each}
</div>

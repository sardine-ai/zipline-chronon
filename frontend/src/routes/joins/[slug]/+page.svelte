<script lang="ts">
	import { Tabs, TabsList, TabsTrigger, TabsContent } from '$lib/components/ui/tabs';
	import PageHeader from '$lib/components/PageHeader/PageHeader.svelte';
	import Observability from '$lib/components/Observability/Observability.svelte';
	import JobTracker from '$lib/components/JobTracker/JobTracker.svelte';
	import { queryParameters, ssp } from 'sveltekit-search-params';

	const { data } = $props();
	const joinTimeseries = $derived(data.joinTimeseries);

	const params = queryParameters({ tab: ssp.string('observability') }, { pushHistory: true });
</script>

<PageHeader title={joinTimeseries.name} />

<Tabs bind:value={params.tab} class="w-full">
	<TabsList>
		<TabsTrigger value="observability">Observability</TabsTrigger>
		<TabsTrigger value="job-tracker">Job Tracker</TabsTrigger>
		<TabsTrigger value="lineage">Lineage</TabsTrigger>
	</TabsList>

	<TabsContent value="observability">
		<Observability {data} />
	</TabsContent>

	<TabsContent value="job-tracker">
		<JobTracker jobTree={data.jobTree} dates={data.dates} />
	</TabsContent>

	<TabsContent value="lineage">
		<div class="py-4">Lineage content coming soon...</div>
	</TabsContent>
</Tabs>

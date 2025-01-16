<script lang="ts">
	import { Tabs, TabsList, TabsTrigger, TabsContent } from '$lib/components/ui/tabs';
	import PageHeader from '$lib/components/PageHeader/PageHeader.svelte';
	import Observability from '$lib/components/Observability/Observability.svelte';

	const { data } = $props();
	const joinTimeseries = $derived(data.joinTimeseries);
</script>

<PageHeader title={joinTimeseries.name} />

<Tabs class="w-full">
	<TabsList>
		<TabsTrigger value="observability">Observability</TabsTrigger>
		<TabsTrigger value="job-tracker">Job Tracker</TabsTrigger>
		<TabsTrigger value="lineage">Lineage</TabsTrigger>
	</TabsList>

	<TabsContent value="observability">
		<Observability {data} />
	</TabsContent>

	<TabsContent value="job-tracker">
		<div class="py-4">Job Tracker content coming soon...</div>
	</TabsContent>

	<TabsContent value="lineage">
		<div class="py-4">Lineage content coming soon...</div>
	</TabsContent>
</Tabs>

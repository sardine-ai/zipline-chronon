<script lang="ts">
	import { page } from '$app/state';

	import { Tabs, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import PageHeader from '$lib/components/PageHeader.svelte';

	const { data, children } = $props();

	const pageName = $derived(page.url.pathname.split('/').at(-1));
</script>

<PageHeader title={data.joinTimeseries.name} />

<Tabs value={pageName} class="w-full">
	<TabsList>
		<TabsTrigger href="/joins/{page.params.slug}/observability">Observability</TabsTrigger>
		<TabsTrigger href="/joins/{page.params.slug}/job-tracker">Job Tracker</TabsTrigger>
		<TabsTrigger href="/joins/{page.params.slug}/lineage">Lineage</TabsTrigger>
	</TabsList>

	{@render children()}
</Tabs>

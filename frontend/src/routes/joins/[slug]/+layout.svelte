<script lang="ts">
	import { page } from '$app/state';

	import { Tabs, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import IconClipboardDocumentCheck16Solid from '~icons/heroicons/clipboard-document-check-16-solid';
	import IconQueueList16Solid from '~icons/heroicons/queue-list-16-solid';
	import IconLineage from '~icons/carbon/ibm-cloud-pak-manta-automated-data-lineage';
	import { Separator } from '$src/lib/components/ui/separator/index.js';

	const { data, children } = $props();

	const pageName = $derived(page.url.pathname.split('/').at(-1));
</script>

<PageHeader title={data.joinTimeseries.name} />

<Tabs value={pageName} class="w-full">
	<TabsList>
		<TabsTrigger href="/joins/{page.params.slug}/observability">
			<IconClipboardDocumentCheck16Solid class="mr-2 h-4 w-4" />
			Observability</TabsTrigger
		>
		<TabsTrigger href="/joins/{page.params.slug}/job-tracking">
			<IconQueueList16Solid class="mr-2 h-4 w-4" />
			Job tracking</TabsTrigger
		>
		<TabsTrigger href="/joins/{page.params.slug}/lineage">
			<IconLineage class="mr-2 h-4 w-4" />
			Lineage</TabsTrigger
		>
	</TabsList>
	<Separator fullWidthExtend={true} class="mt-[2px]" />

	{@render children()}
</Tabs>

<script lang="ts">
	import { page } from '$app/state';
	import { entityConfig, getEntityType } from '$lib/types/Entity';

	import { Tabs, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import IconClipboardDocumentCheck16Solid from '~icons/heroicons/clipboard-document-check-16-solid';
	import IconQueueList16Solid from '~icons/heroicons/queue-list-16-solid';
	import IconLineage from '~icons/carbon/ibm-cloud-pak-manta-automated-data-lineage';
	import { Separator } from '$src/lib/components/ui/separator/index.js';

	const { data, children } = $props();
</script>

<PageHeader
	title={data.conf?.metaData?.name ?? 'Unknown'}
	learnHref={entityConfig[getEntityType(data.conf)].learnHref}
/>

<Tabs class="flex-1 flex flex-col">
	<TabsList class="justify-start">
		<TabsTrigger href="/{page.params.conf}/{page.params.name}/overview">
			<IconLineage class="mr-2 h-4 w-4" />
			Overview
		</TabsTrigger>
		<TabsTrigger href="/{page.params.conf}/{page.params.name}/job-tracking">
			<IconQueueList16Solid class="mr-2 h-4 w-4" />
			Job tracking
		</TabsTrigger>

		<!-- TODO: Hide if user only has access to control plane (and not data plane) -->
		{#if page.params.conf === 'joins'}
			<TabsTrigger href="/{page.params.conf}/{page.params.name}/observability">
				<IconClipboardDocumentCheck16Solid class="mr-2 h-4 w-4" />
				Observability
			</TabsTrigger>
		{/if}
	</TabsList>
	<Separator fullWidthExtend={true} class="mt-[2px]" />

	{@render children()}
</Tabs>

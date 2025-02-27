<script lang="ts">
	import EntityTable from '$lib/components/EntityTable.svelte';
	import { sort } from '@layerstack/utils';

	import PageHeader from '$lib/components/PageHeader.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import { getEntityConfigFromPath } from '$lib/types/Entity';
	import { page } from '$app/state';

	const { data } = $props();

	// TODO: remove this once we have observability data for all joins
	const sortedItems = $derived(
		sort(data.items, (d) => (d.metaData?.name === 'risk.user_transactions.txn_join' ? -1 : 1))
	);

	const entityConfig = $derived(getEntityConfigFromPath(page.url.pathname));
	const learnHref = $derived(data.items?.[0] ? entityConfig?.learnHref : 'https://chronon.ai/');
</script>

{#if entityConfig}
	<PageHeader title={data.title} {learnHref} />

	<Separator fullWidthExtend={true} />
	<EntityTable {entityConfig} items={sortedItems} basePath={data.basePath} />
{:else}
	<PageHeader title="Unknown entity type" />
{/if}

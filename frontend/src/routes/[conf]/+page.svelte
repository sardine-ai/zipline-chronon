<script lang="ts">
	import EntityTable from '$lib/components/EntityTable.svelte';
	import { sort } from '@layerstack/utils';
	import { afterNavigate } from '$app/navigation';

	import PageHeader from '$lib/components/PageHeader.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import { getEntityConfigFromPath } from '$lib/types/Entity';
	import { page } from '$app/state';
	import Input from '$lib/components/ui/input/input.svelte';

	const { data } = $props();
	let searchTerm = $state('');

	// Clear search term after navigation
	afterNavigate(() => {
		searchTerm = '';
	});

	const sortedAndFilteredItems = $derived.by(() => {
		const term = searchTerm.toLowerCase();

		const filtered = searchTerm
			? data.items.filter(
					(item) =>
						item.metaData?.name?.toLowerCase().includes(term) ||
						item.metaData?.team?.toLowerCase().includes(term)
				)
			: data.items;

		// TODO: remove this once we have observability data for all joins
		return sort(filtered, (d) => (d.metaData?.name === 'risk.user_transactions.txn_join' ? -1 : 1));
	});

	const entityConfig = $derived(getEntityConfigFromPath(page.url.pathname));
	const learnHref = $derived(data.items?.[0] ? entityConfig?.learnHref : 'https://chronon.ai/');
</script>

{#if entityConfig}
	<PageHeader title={data.title} {learnHref} />
	<Input
		class="max-w-xs mb-3"
		placeholder="Filter {entityConfig.label}..."
		type="text"
		value={searchTerm}
		on:input={(e) => (searchTerm = e.currentTarget.value)}
	/>
	<Separator fullWidthExtend={true} />
	<EntityTable {entityConfig} items={sortedAndFilteredItems} basePath={data.basePath} />
{:else}
	<PageHeader title="Unknown entity type" />
{/if}

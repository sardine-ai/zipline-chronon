<script lang="ts">
	import EntityTable from '$lib/components/EntityTable.svelte';
	import { sort } from '@layerstack/utils';

	const { data } = $props();

	// TODO: remove this once we have observability data for all joins
	const sortedItems = $derived(
		sort(data.items, (d) => (d.metaData?.name === 'risk.user_transactions.txn_join' ? -1 : 1))
	);
</script>

<EntityTable items={sortedItems} basePath={data.basePath} title={data.title} />

<script lang="ts">
	import LogicalNodeTable from '$lib/components/LogicalNodeTable.svelte';
	import { sort } from '@layerstack/utils';

	const { data } = $props();

	// TODO: remove this once we have data for all joins
	const sortedItems = sort(data.items, (d) =>
		d.metaData?.name === 'risk.user_transactions.txn_join' ? -1 : 1
	);
</script>

<LogicalNodeTable items={sortedItems} basePath={data.basePath} title={data.title} />

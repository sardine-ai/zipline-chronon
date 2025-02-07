<script lang="ts">
	import LogicalNodeTable from '$lib/components/LogicalNodeTable.svelte';
	import type { IJoin } from '$src/lib/types/codegen';

	const { data } = $props();

	// todo: remove this once we have data for all joins
	const sortedItems = data.items.sort((a: IJoin, b: IJoin) => {
		if (a.metaData?.name === 'risk.user_transactions.txn_join') return -1;
		if (b.metaData?.name === 'risk.user_transactions.txn_join') return 1;
		return 0;
	});
</script>

<LogicalNodeTable items={sortedItems} basePath={data.basePath} title={data.title} />

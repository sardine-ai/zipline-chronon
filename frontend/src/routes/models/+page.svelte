<script lang="ts">
	import LogicalNodeTable from '$lib/components/LogicalNodeTable.svelte';

	const { data } = $props();
</script>

<LogicalNodeTable items={data.items} basePath={data.basePath} title={data.title} />

<script lang="ts">
	import { Badge } from '$lib/components/ui/badge';

	let {
		isTrue
	}: {
		isTrue: boolean;
	} = $props();
</script>

{#if isTrue}
	<Badge variant="success">True</Badge>
{:else}
	<Badge variant="outline">False</Badge>
{/if}

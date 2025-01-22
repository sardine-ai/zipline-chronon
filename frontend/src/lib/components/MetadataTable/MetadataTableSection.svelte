<script lang="ts">
	import { Table, TableBody } from '$lib/components/ui/table/index.js';
	import type { Snippet } from 'svelte';

	const { children }: { children: Snippet } = $props();
</script>

<div class="border border-neutral-400 rounded-md">
	<Table density="compact">
		<TableBody>
			{@render children()}
		</TableBody>
	</Table>
</div>

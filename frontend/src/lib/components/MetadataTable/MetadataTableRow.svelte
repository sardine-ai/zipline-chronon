<script lang="ts">
	import { TableCell, TableRow } from '$lib/components/ui/table/index.js';
	import type { Snippet } from 'svelte';

	const { label, value, children }: { label: string; value?: string; children?: Snippet } =
		$props();
</script>

<TableRow class="border-neutral-400">
	<TableCell class="flex justify-between">
		<span class="text-muted-foreground">{label}</span>
		{#if value}
			<div class="truncate max-w-[50%]" title={value}>
				{value}
			</div>
		{/if}
		{#if children}
			{@render children()}
		{/if}
	</TableCell>
</TableRow>

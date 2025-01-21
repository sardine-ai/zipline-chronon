<script lang="ts">
	import type { Snippet } from 'svelte';
	import { cn } from '$lib/utils';

	const {
		columns = 1,
		children,
		class: className = ''
	}: {
		columns: number;
		children: Snippet;
		class?: string;
	} = $props();
</script>

<div class={cn(`grid gap-4 grid-cols-${columns}`, className, columns === 1 ? 'w-1/2' : `w-full`)}>
	{@render children()}
</div>

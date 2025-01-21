<script lang="ts">
	import IconChevronDown from '~icons/heroicons/chevron-down-16-solid';
	import type { Writable } from 'svelte/store';
	import CellDivider from '$lib/components/CellDivider.svelte';
	import Badge from '$lib/components/ui/badge/badge.svelte';

	let {
		isExpanded,
		canExpand,
		depth,
		name,
		childrenCount
	}: {
		isExpanded: Writable<boolean>;
		canExpand: Writable<boolean>;
		depth: number;
		name: string;
		childrenCount: number;
	} = $props();
</script>

<div class="flex items-center justify-center h-full relative">
	<CellDivider />
	<div
		class="flex items-center bg-neutral-300 border border-neutral-400 rounded-md p-[5px] pl-0 w-full mr-[18px]"
		style:margin-left={`calc(${depth * 0.75}rem)`}
	>
		{#if $canExpand}
			<button class="mr-1 ml-1" onclick={() => ($isExpanded = !$isExpanded)}>
				<IconChevronDown
					class="size-4 transition-transform duration-200 text-neutral-900 {$isExpanded
						? ''
						: '-rotate-90'}"
				/>
			</button>
		{/if}
		{#if childrenCount > 0}
			<Badge variant="secondary" class="border-none rounded-xl bg-neutral-200 text-neutral-700">
				{childrenCount}
			</Badge>
		{/if}
		<span class="whitespace-nowrap pl-2 text-neutral-900">{name}</span>
	</div>
</div>

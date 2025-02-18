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
	<button
		class="flex items-center bg-neutral-300 border border-neutral-400 rounded-md p-[5px] pl-0 w-full mr-[18px] max-w-[400px]"
		style:margin-left={`calc(${depth * 0.75}rem)`}
		onclick={() => ($isExpanded = !$isExpanded)}
		title={name}
	>
		{#if $canExpand}
			<IconChevronDown
				class="mr-1 ml-1 size-4 transition-transform duration-200 text-neutral-900 {$isExpanded
					? ''
					: '-rotate-90'}"
			/>
		{/if}
		{#if childrenCount > 0}
			<Badge
				variant="secondary"
				class="border-none rounded-xl bg-neutral-200 text-neutral-700 flex-shrink-0"
			>
				{childrenCount}
			</Badge>
		{/if}
		<span class="whitespace-nowrap pl-2 text-neutral-900 overflow-hidden text-ellipsis">{name}</span
		>
	</button>
</div>

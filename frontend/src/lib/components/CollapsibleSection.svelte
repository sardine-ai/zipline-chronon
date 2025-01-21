<script lang="ts">
	import {
		Collapsible,
		CollapsibleContent,
		CollapsibleTrigger
	} from '$lib/components/ui/collapsible';
	import type { Snippet } from 'svelte';
	import IconChevronDown from '~icons/heroicons/chevron-down-16-solid';

	let {
		collapsibleContent,
		headerContentLeft,
		headerContentRight,
		title,
		open = $bindable(true),
		size = 'default',
		class: className = ''
	}: {
		collapsibleContent: Snippet;
		headerContentLeft?: Snippet;
		headerContentRight?: Snippet;
		title: string;
		open: boolean;
		size?: 'small' | 'default' | 'large';
		class?: string;
	} = $props();

	const sizeClasses = {
		small: {
			wrapper: 'mt-6 mb-8',
			title: 'text-regular'
		},
		default: {
			wrapper: 'my-8',
			title: 'text-large-medium'
		},
		large: {
			wrapper: 'my-12',
			title: 'text-xl'
		}
	}[size];
</script>

<Collapsible bind:open class="{sizeClasses.wrapper} {className}">
	<div class="flex mb-3">
		<CollapsibleTrigger class="flex items-center space-x-4">
			<IconChevronDown
				class="size-4 transition-transform duration-200 {open ? '' : 'rotate-180'}"
			/>
			<h2 class="{sizeClasses.title} !ml-2 select-text">{title}</h2>
		</CollapsibleTrigger>
		<div class="flex items-center justify-between flex-1 ml-2">
			{#if headerContentLeft}
				{@render headerContentLeft()}
			{/if}
			<div></div>
			{#if headerContentRight}
				{@render headerContentRight()}
			{/if}
		</div>
	</div>
	<CollapsibleContent>
		{@render collapsibleContent()}
	</CollapsibleContent>
</Collapsible>

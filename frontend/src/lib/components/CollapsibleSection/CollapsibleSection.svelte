<script lang="ts">
	import {
		Collapsible,
		CollapsibleContent,
		CollapsibleTrigger
	} from '$lib/components/ui/collapsible';
	import type { Snippet } from 'svelte';
	import { ChevronDown } from 'svelte-radix';

	let {
		children,
		title,
		open = $bindable(true)
	}: { children: Snippet; title: string; open: boolean } = $props();
</script>

<Collapsible bind:open class="mt-4">
	<CollapsibleTrigger class="flex items-center w-full">
		<ChevronDown
			class="w-5 h-5 mr-2 transition-transform duration-200 {open ? '' : 'rotate-180'}"
		/>
		<h2 class="text-xl">{title}</h2>
	</CollapsibleTrigger>
	<CollapsibleContent>
		{@render children()}
	</CollapsibleContent>
</Collapsible>

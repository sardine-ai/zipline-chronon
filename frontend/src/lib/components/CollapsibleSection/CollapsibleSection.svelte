<script lang="ts">
	import {
		Collapsible,
		CollapsibleContent,
		CollapsibleTrigger
	} from '$lib/components/ui/collapsible';
	import type { Snippet } from 'svelte';
	import { Icon, ChevronDown } from 'svelte-hero-icons';
	import { stopPropogation } from '$lib/util/event';
	let {
		collabsibleContent,
		headerContentLeft,
		headerContentRight,
		title,
		open = $bindable(true)
	}: {
		collabsibleContent: Snippet;
		headerContentLeft?: Snippet;
		headerContentRight?: Snippet;
		title: string;
		open: boolean;
	} = $props();
</script>

<Collapsible bind:open class="mt-9 mb-6">
	<div class="flex items-center justify-between mb-4">
		<CollapsibleTrigger class="flex items-center space-x-4 w-full">
			<Icon
				src={ChevronDown}
				micro
				size="16"
				class="transition-transform duration-200 {open ? '' : 'rotate-180'}"
			/>
			<h2 class="text-lg">{title}</h2>
			{#if headerContentLeft}
				<!-- svelte-ignore event_directive_deprecated -->
				<div on:click={stopPropogation} role="presentation">
					{@render headerContentLeft()}
				</div>
			{/if}
		</CollapsibleTrigger>
		{#if headerContentRight}
			<div>
				{@render headerContentRight()}
			</div>
		{/if}
	</div>
	<CollapsibleContent>
		{@render collabsibleContent()}
	</CollapsibleContent>
</Collapsible>

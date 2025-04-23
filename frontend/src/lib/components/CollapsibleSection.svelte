<script lang="ts">
	import type { Snippet } from 'svelte';
	import { Collapse } from 'svelte-ux';

	let {
		label,
		children,
		// eslint-disable-next-line @typescript-eslint/no-unused-vars -- Not sure why this is throwing an error
		trigger,
		open = $bindable(false),
		class: className = ''
	}: {
		label?: string;
		children: Snippet;
		trigger?: Snippet;
		open?: boolean;
		class?: string;
	} = $props();
</script>

<Collapse
	{open}
	classes={{
		root: '[&>button]:gap-3',
		icon: 'text-sm order-first data-[open=true]:rotate-0 data-[open=false]:-rotate-90',
		content: 'mt-2'
	}}
	class={className}
>
	{#snippet trigger()}
		{#if trigger}
			{@render trigger()}
		{:else}
			{label}
		{/if}
	{/snippet}

	{@render children()}
</Collapse>

<script lang="ts">
	import type { HTMLTableAttributes } from 'svelte/elements';
	import { cn } from '$lib/utils.js';
	import { tableVariants } from './index.js';

	interface $$Props extends HTMLTableAttributes {
		density?: 'compact' | 'default';
		scrollContainer?: HTMLDivElement;
		hideScrollbar?: boolean;
	}

	let className: $$Props['class'] = undefined;
	export let density: $$Props['density'] = 'default';
	export let scrollContainer: $$Props['scrollContainer'] = undefined;
	export let hideScrollbar: $$Props['hideScrollbar'] = false;
	export { className as class };
</script>

<div
	bind:this={scrollContainer}
	class={cn('relative w-full overflow-auto', hideScrollbar && 'scrollbar-none')}
	on:scroll
>
	<table class={cn(tableVariants({ density }), className)} {...$$restProps}>
		<slot />
	</table>
</div>

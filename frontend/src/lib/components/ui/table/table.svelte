<script lang="ts">
	import type { HTMLTableAttributes } from 'svelte/elements';
	import { cn } from '$lib/utils.js';
	import { tableVariants } from './index.js';

	interface $$Props extends HTMLTableAttributes {
		density?: 'compact' | 'default';
	}

	let className: $$Props['class'] = undefined;
	export let density: $$Props['density'] = 'default';
	export { className as class };
</script>

<div class="relative w-full overflow-auto">
	<table class={cn(tableVariants({ density }), className)} {...$$restProps}>
		<slot />
	</table>
</div>

<script lang="ts">
	import type { HTMLAnchorAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLAnchorAttributes & {
		el?: HTMLAnchorElement;
		asChild?: boolean;
	};

	export let href: $$Props['href'] = undefined;
	export let el: $$Props['el'] = undefined;
	export let asChild: $$Props['asChild'] = false;
	let className: $$Props['class'] = undefined;
	export { className as class };

	let attrs: Record<string, unknown>;

	$: attrs = {
		class: cls('hover:text-foreground transition-colors text-neutral-800', className),
		href,
		...$$restProps
	};
</script>

{#if asChild}
	<slot {attrs} />
{:else}
	<a bind:this={el} {...attrs} {href}>
		<slot {attrs} />
	</a>
{/if}

<script lang="ts">
	import type { HTMLThAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLThAttributes & {
		element?: HTMLElement;
	};

	let className: $$Props['class'] = undefined;
	export { className as class };
	export let element: HTMLElement | undefined = undefined;
</script>

<th
	bind:this={element}
	class={cls(
		'text-muted-foreground h-10 px-3 text-left align-middle text-sm [&:has([role=checkbox])]:pr-0 [&>[role=checkbox]]:translate-y-[2px] border-r last:border-r-0',
		className
	)}
	{...$$restProps}
>
	<slot />
</th>

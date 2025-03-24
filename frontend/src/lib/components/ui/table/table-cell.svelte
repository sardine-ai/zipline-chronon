<script lang="ts">
	import type { HTMLTdAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLTdAttributes;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<td
	class={cls(
		'p-3 align-middle [&:has([role=checkbox])]:pr-0 [&>[role=checkbox]]:translate-y-[2px] border-r last:border-r-0',
		className
	)}
	{...$$restProps}
	on:click
	on:keydown
>
	<slot />
</td>

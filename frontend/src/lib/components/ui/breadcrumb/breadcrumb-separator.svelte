<script lang="ts">
	import type { HTMLLiAttributes } from 'svelte/elements';
	import ChevronRight from 'svelte-radix/ChevronRight.svelte';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLLiAttributes & {
		el?: HTMLLIElement;
	};

	export let el: $$Props['el'] = undefined;
	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<li
	role="presentation"
	aria-hidden="true"
	class={cls('[&>svg]:size-3.5 text-neutral-800', className)}
	bind:this={el}
	{...$$restProps}
>
	<slot>
		<ChevronRight tabindex="-1" />
	</slot>
</li>

<script lang="ts">
	import type { HTMLAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLAttributes<HTMLTableSectionElement>;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<thead class={cls('[&_tr]:border-b', className)} {...$$restProps} on:click on:keydown>
	<slot />
</thead>

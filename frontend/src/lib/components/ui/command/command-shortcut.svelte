<script lang="ts">
	import type { HTMLAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLAttributes<HTMLSpanElement>;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<span
	class={cls('text-muted-foreground ml-auto text-xs tracking-widest', className)}
	{...$$restProps}
>
	<slot />
</span>

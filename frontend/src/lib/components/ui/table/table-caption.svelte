<script lang="ts">
	import type { HTMLAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	type $$Props = HTMLAttributes<HTMLTableCaptionElement>;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<caption class={cls('text-muted-foreground mt-4 text-sm', className)} {...$$restProps}>
	<slot />
</caption>

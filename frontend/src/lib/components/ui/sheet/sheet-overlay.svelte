<script lang="ts">
	import { Dialog as SheetPrimitive } from 'bits-ui';
	import { fade } from 'svelte/transition';
	import { cls } from '@layerstack/tailwind';

	type $$Props = SheetPrimitive.OverlayProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
	export let transition: $$Props['transition'] = fade;
	export let transitionConfig: $$Props['transitionConfig'] = {
		duration: 150
	};
</script>

<SheetPrimitive.Overlay
	{transition}
	{transitionConfig}
	class={cls('bg-background/80 fixed inset-0 z-50 backdrop-blur-xs', className)}
	{...$$restProps}
/>

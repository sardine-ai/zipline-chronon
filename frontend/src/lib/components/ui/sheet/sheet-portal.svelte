<script lang="ts">
	import { Dialog as SheetPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = SheetPrimitive.PortalProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<SheetPrimitive.Portal class={cls(className)} {...$$restProps}>
	<slot />
</SheetPrimitive.Portal>

<script lang="ts">
	import { Dialog as SheetPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = SheetPrimitive.DescriptionProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<SheetPrimitive.Description
	class={cls('text-muted-foreground text-sm', className)}
	{...$$restProps}
>
	<slot />
</SheetPrimitive.Description>

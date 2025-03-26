<script lang="ts">
	import { Dialog as SheetPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = SheetPrimitive.TitleProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<SheetPrimitive.Title
	class={cls('text-foreground text-lg font-semibold', className)}
	{...$$restProps}
>
	<slot />
</SheetPrimitive.Title>

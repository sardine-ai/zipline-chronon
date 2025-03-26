<script lang="ts">
	import { DropdownMenu as DropdownMenuPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = DropdownMenuPrimitive.SeparatorProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<DropdownMenuPrimitive.Separator
	class={cls('bg-muted -mx-1 my-1 h-px', className)}
	{...$$restProps}
/>

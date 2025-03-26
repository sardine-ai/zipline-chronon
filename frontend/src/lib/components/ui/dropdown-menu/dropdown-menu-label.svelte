<script lang="ts">
	import { DropdownMenu as DropdownMenuPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = DropdownMenuPrimitive.LabelProps & {
		inset?: boolean;
	};

	let className: $$Props['class'] = undefined;
	export let inset: $$Props['inset'] = undefined;
	export { className as class };
</script>

<DropdownMenuPrimitive.Label
	class={cls('px-2 py-1.5 text-sm font-semibold', inset && 'pl-8', className)}
	{...$$restProps}
>
	<slot />
</DropdownMenuPrimitive.Label>

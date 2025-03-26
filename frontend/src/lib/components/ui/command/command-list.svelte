<script lang="ts">
	import { Command as CommandPrimitive } from 'cmdk-sv';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CommandPrimitive.ListProps;
	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CommandPrimitive.List
	class={cls('max-h-[300px] overflow-y-auto overflow-x-hidden', className)}
	{...$$restProps}
>
	<slot />
</CommandPrimitive.List>

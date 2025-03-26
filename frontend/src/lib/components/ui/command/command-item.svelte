<script lang="ts">
	import { Command as CommandPrimitive } from 'cmdk-sv';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CommandPrimitive.ItemProps;

	export let asChild = false;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CommandPrimitive.Item
	{asChild}
	class={cls(
		'aria-selected:bg-accent aria-selected:text-accent-foreground relative flex cursor-default select-none items-center px-4! py-1.5 text-sm outline-hidden data-disabled:pointer-events-none data-disabled:opacity-50 [&>svg:first-of-type]:mr-2 bg-neutral-200 h-10 not-last:border-b',
		'data-disabled:cursor-default cursor-pointer',
		className
	)}
	{...$$restProps}
	let:action
	let:attrs
>
	<slot {action} {attrs} />
</CommandPrimitive.Item>

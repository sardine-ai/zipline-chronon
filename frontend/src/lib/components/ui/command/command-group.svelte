<script lang="ts">
	import { Command as CommandPrimitive } from 'cmdk-sv';
	import { cls } from '@layerstack/tailwind';
	type $$Props = CommandPrimitive.GroupProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CommandPrimitive.Group
	class={cls(
		'text-foreground **:data-cmdk-group-heading:text-neutral-700 overflow-hidden p-0! **:data-cmdk-group-heading:px-4 **:data-cmdk-group-heading:py-1 **:data-cmdk-group-heading:text-xs **:data-cmdk-group-heading:font-medium **:data-cmdk-group-heading:h-6 **:data-cmdk-group-heading:bg-neutral-300',
		className
	)}
	{...$$restProps}
>
	<slot />
</CommandPrimitive.Group>

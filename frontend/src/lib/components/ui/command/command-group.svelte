<script lang="ts">
	import { Command as CommandPrimitive } from 'cmdk-sv';
	import { cn } from '$lib/utils.js';
	type $$Props = CommandPrimitive.GroupProps;

	let className: string | undefined | null = undefined;
	export { className as class };
</script>

<CommandPrimitive.Group
	class={cn(
		'text-foreground [&_[data-cmdk-group-heading]]:text-neutral-700 overflow-hidden !p-0 [&_[data-cmdk-group-heading]]:px-4 [&_[data-cmdk-group-heading]]:py-1 [&_[data-cmdk-group-heading]]:text-xs [&_[data-cmdk-group-heading]]:font-medium [&_[data-cmdk-group-heading]]:h-6 [&_[data-cmdk-group-heading]]:bg-neutral-300',
		className
	)}
	{...$$restProps}
>
	<slot />
</CommandPrimitive.Group>

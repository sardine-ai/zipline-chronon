<script lang="ts">
	import { Command as CommandPrimitive } from 'cmdk-sv';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CommandPrimitive.SeparatorProps;
	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CommandPrimitive.Separator class={cls('bg-border -mx-1 h-px', className)} {...$$restProps} />

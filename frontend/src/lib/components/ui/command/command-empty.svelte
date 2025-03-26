<script lang="ts">
	import { Command as CommandPrimitive } from 'cmdk-sv';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CommandPrimitive.EmptyProps;
	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CommandPrimitive.Empty class={cls('py-6 text-center text-sm', className)} {...$$restProps}>
	<slot />
</CommandPrimitive.Empty>

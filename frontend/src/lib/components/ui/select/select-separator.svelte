<script lang="ts">
	import { Select as SelectPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = SelectPrimitive.SeparatorProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<SelectPrimitive.Separator class={cls('bg-muted -mx-1 my-1 h-px', className)} {...$$restProps} />

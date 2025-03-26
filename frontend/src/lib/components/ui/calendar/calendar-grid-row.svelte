<script lang="ts">
	import { Calendar as CalendarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CalendarPrimitive.GridRowProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CalendarPrimitive.GridRow class={cls('flex', className)} {...$$restProps}>
	<slot />
</CalendarPrimitive.GridRow>

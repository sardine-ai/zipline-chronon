<script lang="ts">
	import { Calendar as CalendarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CalendarPrimitive.GridProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CalendarPrimitive.Grid class={cls('w-full border-collapse space-y-1', className)} {...$$restProps}>
	<slot />
</CalendarPrimitive.Grid>

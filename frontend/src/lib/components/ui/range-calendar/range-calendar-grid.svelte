<script lang="ts">
	import { RangeCalendar as RangeCalendarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = RangeCalendarPrimitive.GridProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<RangeCalendarPrimitive.Grid
	class={cls('w-full border-collapse space-y-1', className)}
	{...$$restProps}
>
	<slot />
</RangeCalendarPrimitive.Grid>

<script lang="ts">
	import { RangeCalendar as RangeCalendarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = RangeCalendarPrimitive.HeaderProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<RangeCalendarPrimitive.Header
	class={cls('relative flex w-full items-center justify-between pt-1', className)}
	{...$$restProps}
>
	<slot />
</RangeCalendarPrimitive.Header>

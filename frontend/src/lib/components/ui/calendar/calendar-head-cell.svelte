<script lang="ts">
	import { Calendar as CalendarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CalendarPrimitive.HeadCellProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CalendarPrimitive.HeadCell
	class={cls('text-muted-foreground w-8 rounded-md text-[0.8rem] font-normal', className)}
	{...$$restProps}
>
	<slot />
</CalendarPrimitive.HeadCell>

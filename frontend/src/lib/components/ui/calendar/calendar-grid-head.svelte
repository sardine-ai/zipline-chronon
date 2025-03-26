<script lang="ts">
	import { Calendar as CalendarPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = CalendarPrimitive.GridHeadProps;

	let className: $$Props['class'] = undefined;
	export { className as class };
</script>

<CalendarPrimitive.GridHead class={cls(className)} {...$$restProps}>
	<slot />
</CalendarPrimitive.GridHead>

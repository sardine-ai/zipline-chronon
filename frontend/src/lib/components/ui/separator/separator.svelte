<script lang="ts">
	import { Separator as SeparatorPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';

	type $$Props = SeparatorPrimitive.Props & { fullWidthExtend?: boolean; wide?: boolean };

	let className: $$Props['class'] = undefined;
	export let orientation: $$Props['orientation'] = 'horizontal';
	export let decorative: $$Props['decorative'] = undefined;
	export let fullWidthExtend: $$Props['fullWidthExtend'] = false;
	export let wide: $$Props['wide'] = false;
	export { className as class };
</script>

<SeparatorPrimitive.Root
	class={cls(
		'bg-border shrink-0',
		orientation === 'horizontal' ? 'h-[1px] w-full' : 'min-h-full w-[1px]',
		fullWidthExtend && 'relative w-[calc(100%+4rem)] -mx-8',
		wide && 'h-[6px] bg-background border-y border-border',
		className
	)}
	{orientation}
	{decorative}
	{...$$restProps}
/>

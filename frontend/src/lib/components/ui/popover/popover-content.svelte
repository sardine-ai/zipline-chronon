<script lang="ts">
	import { Popover as PopoverPrimitive } from 'bits-ui';
	import { cn, flyAndScale } from '$lib/utils.js';

	type $$Props = PopoverPrimitive.ContentProps & {
		variant?: 'default' | 'light';
	};

	let className: $$Props['class'] = undefined;
	export let transition: $$Props['transition'] = flyAndScale;
	export let transitionConfig: $$Props['transitionConfig'] = undefined;
	export let align: $$Props['align'] = 'center';
	export let sideOffset: $$Props['sideOffset'] = 4;
	export let variant: $$Props['variant'] = 'default';
	export { className as class };
</script>

<PopoverPrimitive.Content
	{transition}
	{transitionConfig}
	{align}
	{sideOffset}
	{...$$restProps}
	class={cn(
		'z-50 w-72 rounded-md border p-4 shadow-md outline-hidden',
		variant === 'default' && 'bg-popover text-popover-foreground',
		variant === 'light' && 'bg-neutral-100 border-neutral-400',
		className
	)}
>
	<slot />
</PopoverPrimitive.Content>

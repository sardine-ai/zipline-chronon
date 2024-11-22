<script lang="ts">
	import { Tabs as TabsPrimitive } from 'bits-ui';
	import { cn } from '$lib/utils.js';

	type $$Props = TabsPrimitive.TriggerProps;
	type $$Events = TabsPrimitive.TriggerEvents;

	let className: $$Props['class'] = undefined;
	export let value: $$Props['value'];
	export { className as class };
</script>

<TabsPrimitive.Trigger
	class={cn(
		'ring-offset-background focus-visible:ring-ring border-b-[3px] border-transparent data-[state=active]:border-b-[3px] data-[state=active]:border-primary-800 data-[state=active]:text-primary-800 inline-flex items-center justify-center whitespace-nowrap px-3 py-2 text-regular transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 data-[state=active]:shadow z-10',
		className
	)}
	{value}
	{...$$restProps}
	on:click
	on:keydown
	on:focus
>
	<slot />
</TabsPrimitive.Trigger>

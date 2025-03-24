<script lang="ts">
	import { Tabs as TabsPrimitive } from 'bits-ui';
	import { cls } from '@layerstack/tailwind';
	import { page } from '$app/stores';

	type $$Props = Omit<TabsPrimitive.TriggerProps, 'value'> & {
		value?: TabsPrimitive.TriggerProps['value'];
		href?: string;
	};
	type $$Events = TabsPrimitive.TriggerEvents;

	let className: $$Props['class'] = undefined;
	export let value: $$Props['value'] = undefined;
	export let href: $$Props['href'] = undefined;
	export { className as class };

	const _class = cls(
		'ring-offset-background focus-visible:ring-ring border-b-[3px] border-transparent data-[state=active]:border-b-[3px] data-[state=active]:border-primary-600 dark:data-[state=active]:border-primary-800 data-[state=active]:text-primary-600 dark:data-[state=active]:text-primary-800 inline-flex items-center justify-center whitespace-nowrap px-[10px] py-[5px] text-sm transition-all focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 z-10 not-first:ml-5',
		className
	);

	$: isActive =
		href === '/' ? $page.url.pathname === href : $page.url.pathname.match(href + '($|\\/)') != null;
</script>

{#if value}
	<TabsPrimitive.Trigger class={_class} {value} {...$$restProps} on:click on:keydown on:focus>
		<slot />
	</TabsPrimitive.Trigger>
{:else if href}
	<a
		{href}
		class={_class}
		{...$$restProps}
		role="tab"
		aria-selected={isActive}
		aria-controls={href?.replace(/\//g, '-')}
		data-state={isActive ? 'active' : 'inactive'}
		on:click
		on:keydown
		on:focus
	>
		<slot />
	</a>
{/if}

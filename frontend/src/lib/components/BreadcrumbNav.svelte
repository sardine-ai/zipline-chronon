<script lang="ts">
	import type { ComponentProps } from 'svelte';
	import { Breadcrumb } from 'svelte-ux';
	import IconSlash from '~icons/heroicons/slash';

	import { page } from '$app/state';

	const props: ComponentProps<Breadcrumb<(typeof items)[number]>> = $props();

	const items = $derived([
		{
			label: 'Zipline AI',
			href: '/'
		},
		...page.url.pathname
			.split('/')
			.filter(Boolean)
			.map((path, i, arr) => {
				return {
					label: path,
					href: i === arr.length - 1 ? null : `/${arr.slice(0, i + 1).join('/')}`
				};
			})
	]);
</script>

<Breadcrumb {items} class="gap-2 text-sm" {...props}>
	<span slot="item" let:item>
		{#if item.href}
			<a
				href={item.href}
				class="text-neutral-800 capitalize transition-colors hover:text-foreground"
			>
				{item.label}
			</a>
		{:else}
			<span class="text-neutral-700 capitalize">
				{item.label}
			</span>
		{/if}
	</span>

	<IconSlash slot="divider" class="text-surface-content/25" />
</Breadcrumb>

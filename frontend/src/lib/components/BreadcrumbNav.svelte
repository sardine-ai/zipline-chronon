<script lang="ts">
	import {
		Breadcrumb,
		BreadcrumbItem,
		BreadcrumbLink,
		BreadcrumbPage,
		BreadcrumbSeparator
	} from '$lib/components/ui/breadcrumb';
	import BreadcrumbList from '$lib/components/ui/breadcrumb/breadcrumb-list.svelte';
	import IconSlash from '~icons/heroicons/slash';

	const { breadcrumbs, class: className }: { breadcrumbs: string[]; class?: string } = $props();
</script>

<Breadcrumb class={className}>
	<BreadcrumbList>
		<BreadcrumbItem>
			<BreadcrumbLink href="/">Zipline AI</BreadcrumbLink>
		</BreadcrumbItem>
		{#each breadcrumbs as crumb, index}
			<BreadcrumbSeparator>
				<IconSlash />
			</BreadcrumbSeparator>
			<BreadcrumbItem>
				{#if index === breadcrumbs.length - 1}
					<BreadcrumbPage class="capitalize">{crumb}</BreadcrumbPage>
				{:else}
					<BreadcrumbLink href="/{breadcrumbs.slice(0, index + 1).join('/')}" class="capitalize">
						{crumb}
					</BreadcrumbLink>
				{/if}
			</BreadcrumbItem>
		{/each}
	</BreadcrumbList>
</Breadcrumb>

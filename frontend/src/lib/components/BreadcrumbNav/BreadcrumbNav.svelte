<script lang="ts">
	import {
		Breadcrumb,
		BreadcrumbItem,
		BreadcrumbLink,
		BreadcrumbPage,
		BreadcrumbSeparator
	} from '$lib/components/ui/breadcrumb';
	import BreadcrumbList from '$lib/components/ui/breadcrumb/breadcrumb-list.svelte';
	import Slash from 'svelte-radix/Slash.svelte';

	const { breadcrumbs }: { breadcrumbs: string[] } = $props();
</script>

<Breadcrumb class="mb-4">
	<BreadcrumbList>
		<BreadcrumbItem>
			<BreadcrumbLink href="/">Home</BreadcrumbLink>
		</BreadcrumbItem>
		{#each breadcrumbs as crumb, index}
			<BreadcrumbSeparator>
				<Slash />
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

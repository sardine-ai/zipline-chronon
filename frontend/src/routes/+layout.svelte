<script lang="ts">
	import '../app.css';
	import { type Snippet } from 'svelte';
	import { page } from '$app/stores';
	import NavigationSlider from '$lib/components/NavigationSlider.svelte';
	import NavigationBar from '$lib/components/NavigationBar.svelte';
	import BreadcrumbNav from '$lib/components/BreadcrumbNav.svelte';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { entityConfig } from '$lib/types/Entity/Entity';

	let { children }: { children: Snippet } = $props();

	// TODO: Replace with actual user data when implemented
	const user = {
		name: 'Demo User',
		avatar: ''
	};

	const breadcrumbs = $derived($page.url.pathname.split('/').filter(Boolean));
</script>

<div class="flex h-screen">
	<NavigationSlider />

	<!-- Left navigation -->
	<NavigationBar navItems={[...entityConfig]} {user} />
	<!-- Main content -->
	<main
		class="flex-1 flex flex-col overflow-hidden bg-neutral-100 relative rounded-tl-xl"
		data-testid="app-main"
	>
		<div class="absolute top-0 bottom-0 w-px bg-border"></div>
		<div
			class="absolute top-0 left-0 w-full h-4 border-l border-t border-border rounded-tl-xl z-20"
		></div>
		<ScrollArea class="flex-1">
			<div class="px-8 py-5">
				<BreadcrumbNav {breadcrumbs} class="mb-[38px]" />
				{@render children()}
			</div>
		</ScrollArea>
	</main>
</div>

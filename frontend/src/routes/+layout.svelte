<script lang="ts">
	import '../app.css';
	import { type Snippet } from 'svelte';
	import { page } from '$app/stores';
	import NavigationSlider from '$lib/components/NavigationSlider/NavigationSlider.svelte';
	import NavigationBar from '$lib/components/NavigationBar/NavigationBar.svelte';
	import BreadcrumbNav from '$lib/components/BreadcrumbNav/BreadcrumbNav.svelte';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { Cube, PuzzlePiece, Square3Stack3d } from 'svelte-hero-icons';

	let { children }: { children: Snippet } = $props();

	// TODO: Replace with actual user data
	const user = {
		name: 'Demo User',
		avatar: '/path/to/avatar.jpg'
	};

	const navItems = [
		{ label: 'Models', href: '/models', icon: Cube },
		{ label: 'GroupBys', href: '/groupbys', icon: PuzzlePiece },
		{ label: 'Joins', href: '/joins', icon: Square3Stack3d }
	];

	const breadcrumbs = $derived($page.url.pathname.split('/').filter(Boolean));
</script>

<div class="flex h-screen">
	<NavigationSlider />

	<!-- Left navigation -->
	<NavigationBar {navItems} {user} />

	<!-- Main content -->
	<main
		class="flex-1 flex flex-col overflow-hidden bg-muted/30 relative rounded-tl-xl"
		data-testid="app-main"
	>
		<ScrollArea class="flex-1 border-l border-t border-border rounded-tl-xl">
			<div class="px-8 py-5">
				<BreadcrumbNav {breadcrumbs} class="mb-10" />
				{@render children()}
			</div>
		</ScrollArea>
	</main>
</div>

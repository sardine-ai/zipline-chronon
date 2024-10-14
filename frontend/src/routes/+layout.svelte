<script lang="ts">
	import '../app.css';
	import { type Snippet } from 'svelte';
	import { page } from '$app/stores';
	import NavigationSlider from '$lib/components/NavigationSlider/NavigationSlider.svelte';
	import NavigationBar from '$lib/components/NavigationBar/NavigationBar.svelte';
	import BreadcrumbNav from '$lib/components/BreadcrumbNav/BreadcrumbNav.svelte';

	let { children }: { children: Snippet } = $props();

	// TODO: Replace with actual user data
	const user = {
		name: 'Demo User',
		avatar: '/path/to/avatar.jpg'
	};

	// TODO: Replace with actual navigation items
	const navItems = [
		{ label: 'Models', href: '/models' },
		{ label: 'GroupBys', href: '/groupbys' },
		{ label: 'Joins', href: '/joins' }
	];

	const breadcrumbs = $derived($page.url.pathname.split('/').filter(Boolean));
</script>

<div class="flex h-screen">
	<NavigationSlider />

	<!-- Left navigation -->
	<NavigationBar {navItems} {user} />

	<!-- Main content -->
	<main
		class="flex-1 overflow-auto p-4 bg-muted/30 rounded-tl-xl border-l border-border"
		data-testid="app-main"
	>
		<BreadcrumbNav {breadcrumbs} />
		{@render children()}
	</main>
</div>

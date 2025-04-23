<script lang="ts">
	import { type Snippet } from 'svelte';
	import { settings, ThemeInit, ThemeSelect } from 'svelte-ux';

	import NavigationSlider from '$lib/components/NavigationSlider.svelte';
	import NavigationMenu from '$lib/components/NavigationMenu.svelte';
	import BreadcrumbNav from '$lib/components/BreadcrumbNav.svelte';

	import '../app.css';

	let { children }: { children: Snippet } = $props();

	// TODO: Replace with actual user data when implemented
	const user = {
		name: 'Demo User',
		avatar: ''
	};

	settings();
</script>

<ThemeInit />

<div class="flex h-screen">
	<NavigationSlider />
	<NavigationMenu {user} />
	<main
		class="flex-1 flex flex-col overflow-y-auto bg-neutral-50 dark:bg-neutral-100 relative rounded-tl-xl"
		data-testid="app-main"
	>
		<div class="absolute top-0 bottom-0 w-px bg-border"></div>
		<div
			class="absolute top-0 left-0 w-full h-4 border-l border-t border-border rounded-tl-xl z-20"
		></div>
		<div class="px-8 flex-1 flex flex-col">
			<div class="flex-1 flex flex-col py-5">
				<div class="flex justify-between items-start mb-3">
					<BreadcrumbNav />
					<ThemeSelect />
				</div>
				{@render children()}
			</div>
		</div>
	</main>
</div>

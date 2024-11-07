<script lang="ts">
	import type { Snippet } from 'svelte';
	import Button from '$lib/components/ui/button/button.svelte';
	import { Icon, BookOpen } from 'svelte-hero-icons';

	let { title, children }: { title: string; children?: Snippet } = $props();

	function openChronon() {
		window.open('https://chronon.ai/', '_blank');
	}
</script>

<div class="flex justify-between items-center mb-6">
	<h1 class="text-3xl">{title}</h1>
	<div class="flex items-center gap-4">
		{#if children}
			{@render children()}
		{/if}
		<Button variant="outline" onclick={openChronon}>
			<Icon src={BookOpen} micro size="16" class="mr-2" />
			Learn
		</Button>
	</div>
</div>

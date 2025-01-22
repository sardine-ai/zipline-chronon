<script lang="ts">
	import type { Snippet } from 'svelte';
	import Button from '$lib/components/ui/button/button.svelte';
	import IconBookOpen from '~icons/heroicons/book-open-16-solid';

	let { title, children }: { title: string; children?: Snippet } = $props();

	function openChronon() {
		window.open('https://chronon.ai/', '_blank');
	}
</script>

<div class="flex justify-between items-center mb-5">
	<h1 class="text-2xl-medium">{title}</h1>
	<div class="flex items-center gap-4">
		{#if children}
			{@render children()}
		{/if}
		<Button variant="outline" onclick={openChronon} icon="leading" size="sm">
			<IconBookOpen class="text-neutral-800" />
			<span class="text-neutral-800">Learn</span>
		</Button>
	</div>
</div>

<script lang="ts">
	import type { Snippet } from 'svelte';
	import { Button } from 'svelte-ux';
	import IconBookOpen from '~icons/heroicons/book-open-16-solid';

	let { title, learnHref, children }: { title: string; learnHref?: string; children?: Snippet } =
		$props();
</script>

<div class="flex justify-between items-center mb-5">
	<h1 class="text-4xl font-medium">{title}</h1>
	<div class="flex items-center gap-4">
		{#if children}
			{@render children()}
		{/if}

		{#if learnHref}
			<Button
				href={learnHref}
				target="_blank"
				class="border border-surface-content/10 rounded-md text-xs text-surface-content/70"
			>
				<IconBookOpen class="text-surface-content/70" />
				Learn
			</Button>
		{/if}
	</div>
</div>

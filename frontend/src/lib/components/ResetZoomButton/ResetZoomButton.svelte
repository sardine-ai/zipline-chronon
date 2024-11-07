<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Icon, ArrowUturnLeft } from 'svelte-hero-icons';

	const { onClick }: { onClick: () => void } = $props();
</script>

<Button on:click={onClick} variant="outline" class="mr-4">
	<Icon src={ArrowUturnLeft} micro size="16" class="mr-2" />
	Reset Zoom
</Button>

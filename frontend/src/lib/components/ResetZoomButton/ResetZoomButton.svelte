<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { cn } from '$lib/utils';
	import { Icon, ArrowUturnLeft } from 'svelte-hero-icons';

	const { onClick, class: className }: { onClick: () => void; class?: string } = $props();
</script>

<Button on:click={onClick} variant="outline" icon="leading" class={cn('', className)} size="sm">
	<Icon src={ArrowUturnLeft} micro size="16" />
	Reset Zoom
</Button>

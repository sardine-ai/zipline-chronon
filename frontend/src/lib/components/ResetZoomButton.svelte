<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { cn } from '$lib/utils';
	import IconArrowUturnLeft from '~icons/heroicons/arrow-uturn-left-16-solid';

	const { onClick, class: className }: { onClick: () => void; class?: string } = $props();
</script>

<Button on:click={onClick} variant="outline" icon="leading" class={cn('', className)} size="sm">
	<IconArrowUturnLeft />
	Reset Zoom
</Button>

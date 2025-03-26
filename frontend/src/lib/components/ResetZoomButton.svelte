<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import IconArrowUturnLeft from '~icons/heroicons/arrow-uturn-left-16-solid';

	const { onClick, class: className }: { onClick: () => void; class?: string } = $props();
</script>

<Button on:click={onClick} variant="outline" icon="leading" class={className} size="sm">
	<IconArrowUturnLeft />
	Reset Zoom
</Button>

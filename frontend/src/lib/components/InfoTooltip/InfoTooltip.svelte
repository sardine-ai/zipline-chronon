<script lang="ts">
	import { Tooltip, TooltipContent, TooltipTrigger } from '$lib/components/ui/tooltip';
	import { Icon, InformationCircle } from 'svelte-hero-icons';

	let { text, maxWidth = '321px' }: { text: string; maxWidth?: string } = $props();
</script>

<Tooltip openDelay={100} closeOnPointerDown={false}>
	<TooltipTrigger>
		<Icon src={InformationCircle} micro size="16" class="text-muted-icon-neutral" />
	</TooltipTrigger>
	<TooltipContent side="bottom" align="start" style="max-width: {maxWidth};">
		<p style="max-width: {maxWidth};">
			{text}
		</p>
	</TooltipContent>
</Tooltip>

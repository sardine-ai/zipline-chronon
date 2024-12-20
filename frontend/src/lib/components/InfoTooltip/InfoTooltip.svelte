<script lang="ts">
	import { Tooltip, TooltipContent, TooltipTrigger } from '$lib/components/ui/tooltip';
	import IconInformationCircle from '~icons/heroicons/information-circle-16-solid';

	let { text, maxWidth = '321px' }: { text: string; maxWidth?: string } = $props();
</script>

<Tooltip openDelay={100} closeOnPointerDown={false}>
	<TooltipTrigger>
		<IconInformationCircle class="text-muted-icon-neutral" />
	</TooltipTrigger>
	<TooltipContent side="bottom" align="start" style="max-width: {maxWidth};">
		<p style="max-width: {maxWidth};">
			{text}
		</p>
	</TooltipContent>
</Tooltip>

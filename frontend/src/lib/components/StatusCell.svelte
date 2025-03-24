<script lang="ts">
	import { statusText } from '$lib/job/status';
	import * as Popover from '$lib/components/ui/popover';
	import { Button } from '$lib/components/ui/button';
	import IconArrowDownOnSquare16Solid from '~icons/heroicons/arrow-down-on-square-16-solid';
	import { format, PeriodType } from '@layerstack/utils';
	import { Status } from '$lib/types/codegen';
	import StatusBar from '$lib/components/StatusBar.svelte';

	let {
		status,
		endDate
	}: {
		status: Status | undefined;
		endDate: string;
	} = $props();

	let isPopoverOpen = $state(false);
</script>

<Popover.Root closeFocus={null} bind:open={isPopoverOpen}>
	<Popover.Trigger class="w-full h-full">
		<div class="flex items-center justify-center bg-neutral-50 pt-1 pb-3">
			<StatusBar {status} />
		</div>
	</Popover.Trigger>
	<Popover.Content sideOffset={-16} variant="light">
		<div>
			<p>
				<span class="mr-1 capitalize">{status ? statusText[status] : ''}</span><span
					class="text-neutral-800">{format(endDate, PeriodType.Day, { variant: 'long' })}</span
				>
			</p>
			<div class="mt-4">
				<Button variant="secondary" class="text-xs! text-neutral-800!">
					<IconArrowDownOnSquare16Solid class="mr-2 h-4 w-4" />
					Download Logs
				</Button>
			</div>
		</div>
	</Popover.Content>
</Popover.Root>

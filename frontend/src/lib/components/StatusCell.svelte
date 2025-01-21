<script lang="ts">
	import {
		statusActiveBorderColors,
		statusBorderColors,
		statusColors,
		type JobStatus
	} from '$lib/types/Job/Job';
	import * as Popover from '$lib/components/ui/popover';
	import { Button } from '$lib/components/ui/button';
	import IconArrowDownOnSquare16Solid from '~icons/heroicons/arrow-down-on-square-16-solid';
	import { format, PeriodType } from '@layerstack/utils';

	let {
		status,
		endDate
	}: {
		status: JobStatus;
		endDate: string;
	} = $props();

	let isPopoverOpen = $state(false);
</script>

<Popover.Root closeFocus={null} bind:open={isPopoverOpen}>
	<Popover.Trigger class="w-full h-full">
		<div class="flex items-center justify-center bg-neutral-50 pt-1 pb-3">
			<div
				class={`h-4 w-full rounded border ${statusColors[status]} hover:${statusActiveBorderColors[status]} ${
					isPopoverOpen ? `${statusActiveBorderColors[status]}` : `${statusBorderColors[status]}`
				} transition-all`}
			></div>
		</div>
	</Popover.Trigger>
	<Popover.Content sideOffset={-16} variant="light">
		<div>
			<p>
				<span class="mr-1 capitalize">{status}</span><span class="text-neutral-800"
					>{format(endDate, PeriodType.Day, { variant: 'long' })}</span
				>
			</p>
			<div class="mt-4">
				<Button variant="secondary" class="!text-small !text-neutral-800">
					<IconArrowDownOnSquare16Solid class="mr-2 h-4 w-4" />
					Download Logs
				</Button>
			</div>
		</div>
	</Popover.Content>
</Popover.Root>

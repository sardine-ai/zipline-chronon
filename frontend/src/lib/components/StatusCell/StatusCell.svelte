<script lang="ts">
	import {
		statusBorderColors,
		statusColors,
		statusRingColors,
		type JobStatus
	} from '$lib/types/Job/Job';
	import * as Popover from '$lib/components/ui/popover';
	import LogsDownloadButton from '$lib/components/LogsDownloadButton/LogsDownloadButton.svelte';

	let {
		status,
		startDate,
		endDate
	}: {
		status: JobStatus;
		startDate: string;
		endDate: string;
	} = $props();

	let isPopoverOpen = $state(false);
</script>

<Popover.Root closeFocus={null} bind:open={isPopoverOpen}>
	<Popover.Trigger class="w-full h-full">
		<div class="flex items-center justify-center bg-neutral-50 pt-1 pb-3">
			<div
				class={`h-4 w-full ${statusColors[status]} ${statusBorderColors[status]} rounded z-10 hover:z-20 hover:ring-2 ${statusRingColors[status]} ${
					isPopoverOpen && `z-20 ring-2 ${statusRingColors[status]}`
				} transition-all`}
			></div>
		</div>
	</Popover.Trigger>
	<Popover.Content sideOffset={-16}>
		<div>
			<p><span class="font-medium">Status:</span> <span class="capitalize">{status}</span></p>
			<p><span class="font-medium">Start:</span> {startDate}</p>
			<p><span class="font-medium">End:</span> {endDate}</p>
			<div class="mt-2">
				<LogsDownloadButton />
			</div>
		</div>
	</Popover.Content>
</Popover.Root>

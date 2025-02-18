<script lang="ts">
	import type { JobTreeNode } from '$lib/job/tree-builder/tree-builder';
	import StatusBar from '$lib/components/StatusBar.svelte';

	let {
		job,
		dates,
		getDaysInRange
	}: {
		job: JobTreeNode;
		dates: string[];
		getDaysInRange: (start: string, end: string, availableDates: string[]) => number;
	} = $props();
</script>

<div class="flex items-center bg-transparent w-full h-full">
	{#if job}
		<div class="flex items-center w-full">
			{#each job.jobTracker?.tasksByDate ?? [] as task}
				{@const daysInRange = getDaysInRange(
					task?.dateRange?.startDate ?? '',
					task?.dateRange?.endDate ?? '',
					dates
				)}
				{@const widthPercentage = (daysInRange / dates.length) * 100}
				<div style="width: {widthPercentage}%">
					<StatusBar status={task.status} />
				</div>
			{/each}
		</div>
	{:else}
		<span class="text-sm text-neutral-600">No data available</span>
	{/if}
</div>

<script lang="ts">
	import type { JobTreeNode } from '$lib/types/Job/Job';
	import { statusColors, statusBorderColors } from '$lib/types/Job/Job';

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

<div class="flex items-center px-2 bg-transparent w-full h-full">
	{#if job}
		<div class="flex items-center w-full">
			{#each job.runs as run}
				{@const daysInRange = getDaysInRange(run.start, run.end, dates)}
				{@const widthPercentage = (daysInRange / dates.length) * 100}
				<div
					class={`h-4 rounded-sm border ${statusColors[run.status]} ${statusBorderColors[run.status]}`}
					style="width: {widthPercentage}%"
				></div>
			{/each}
		</div>
	{:else}
		<span class="text-sm text-neutral-600">No data available</span>
	{/if}
</div>

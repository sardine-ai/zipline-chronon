<script lang="ts">
	import { Status } from '$lib/types/codegen';
	import JobStatusRect from './JobStatusRect.svelte';

	const statuses = Object.values(Status)
		.filter((status): status is Status => typeof status === 'number')
		.sort((a, b) => getStatusLabel(b).length - getStatusLabel(a).length);

	const midPoint = Math.ceil(statuses.length / 2);
	const longLabels = statuses.slice(0, midPoint);
	const shortLabels = statuses.slice(midPoint);

	function getStatusLabel(status: Status) {
		const statusText = Status[status];
		const words = statusText.split('_').map((word) => word.toLowerCase());
		return words.map((word) => word.charAt(0).toUpperCase() + word.slice(1)).join(' ');
	}
</script>

<div class="flex gap-8">
	<div class="flex flex-col gap-2">
		{#each shortLabels as status}
			<div class="flex items-center gap-2 whitespace-nowrap">
				<JobStatusRect {status} class="size-4" />
				<span class="text-sm text-neutral-700">{getStatusLabel(status)}</span>
			</div>
		{/each}
	</div>

	<div class="flex flex-col gap-2">
		{#each longLabels as status}
			<div class="flex items-center gap-2 whitespace-nowrap">
				<JobStatusRect {status} class="size-4" />
				<span class="text-sm text-neutral-700">{getStatusLabel(status)}</span>
			</div>
		{/each}
	</div>
</div>

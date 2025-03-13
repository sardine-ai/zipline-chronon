<script lang="ts">
	import { Status } from '$lib/types/codegen';
	import StatusBar from './StatusBar.svelte';
	import { statusText } from '$lib/job/status';

	const statuses = Object.values(Status)
		.filter((status): status is Status => typeof status === 'number')
		.sort((a, b) => statusText[b].length - statusText[a].length);

	const midPoint = Math.ceil(statuses.length / 2);
	const longLabels = statuses.slice(0, midPoint);
	const shortLabels = statuses.slice(midPoint);
</script>

<div class="flex gap-4">
	<div class="flex flex-col gap-2">
		{#each longLabels as status}
			<div class="flex items-center gap-2 whitespace-nowrap">
				<div class="w-8">
					<StatusBar {status} />
				</div>
				<span class="text-sm text-neutral-700">{statusText[status]}</span>
			</div>
		{/each}
	</div>
	<div class="flex flex-col gap-2">
		{#each shortLabels as status}
			<div class="flex items-center gap-2 whitespace-nowrap">
				<div class="w-8">
					<StatusBar {status} />
				</div>
				<span class="text-sm text-neutral-700">{statusText[status]}</span>
			</div>
		{/each}
	</div>
</div>

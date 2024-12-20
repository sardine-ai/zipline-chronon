<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { cn } from '$lib/utils';

	import IconArrowsUpDown from '~icons/heroicons/arrows-up-down-16-solid';
	import IconPlus from '~icons/heroicons/plus-16-solid';
	import IconSquare3Stack3d from '~icons/heroicons/square-3-stack-3d-16-solid';
	import IconXMark from '~icons/heroicons/x-mark-16-solid';

	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import {
		getSortDirection,
		updateContextSort,
		type SortDirection,
		type SortContext
	} from '$lib/util/sort';

	let {
		showCluster = false,
		class: className,
		showSort = false,
		context = 'drift'
	}: {
		showCluster?: boolean;
		class?: string;
		showSort?: boolean;
		context?: SortContext;
	} = $props();

	let activeCluster = showCluster ? 'GroupBys' : null;

	let currentSort: SortDirection = $derived.by(() =>
		getSortDirection($page.url.searchParams, context)
	);

	function handleSort() {
		const newSort: SortDirection = currentSort === 'asc' ? 'desc' : 'asc';
		const url = updateContextSort($page.url, context, newSort);
		goto(url, { replaceState: true });
	}
</script>

<div class={cn('flex flex-wrap gap-3', className)}>
	<!-- Active Items Section -->
	{#if activeCluster}
		<div class="flex flex-wrap gap-[1px]">
			<Button variant="secondaryAlt" size="sm" icon="leading" disabled>
				<IconSquare3Stack3d />
				Cluster by
			</Button>
			<Button variant="secondaryAlt" size="sm" disabled>
				{activeCluster}
			</Button>
			<Button variant="secondaryAlt" size="sm" class="p-2" disabled>
				<IconXMark />
			</Button>
		</div>
	{/if}

	<!-- Action Buttons Section -->
	<div class="flex gap-3">
		{#if showSort}
			<Button variant="secondary" size="sm" icon="leading" on:click={handleSort}>
				<IconArrowsUpDown />
				Sort {currentSort === 'asc' ? 'A-Z' : 'Z-A'}
			</Button>
		{/if}
		<Button variant="secondary" size="sm" icon="leading" disabled>
			<IconPlus />
			Filter
		</Button>
		{#if showCluster}
			<Button variant="secondary" size="sm" icon="leading" disabled>
				<IconSquare3Stack3d />
				Cluster
			</Button>
		{/if}
	</div>
</div>

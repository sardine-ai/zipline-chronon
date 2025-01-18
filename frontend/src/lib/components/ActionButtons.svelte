<script lang="ts">
	import { queryParameters } from 'sveltekit-search-params';

	import { Button } from '$lib/components/ui/button';
	import { cn } from '$lib/utils';

	import IconArrowsUpDown from '~icons/heroicons/arrows-up-down-16-solid';
	import IconPlus from '~icons/heroicons/plus-16-solid';
	import IconSquare3Stack3d from '~icons/heroicons/square-3-stack-3d-16-solid';
	import IconXMark from '~icons/heroicons/x-mark-16-solid';

	import { getSortParamKey, type SortContext, getSortParamsConfig } from '$lib/util/sort';

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

	const sortKey = $derived(getSortParamKey(context));
	const params = $derived(
		queryParameters(getSortParamsConfig(context), { pushHistory: false, showDefaults: false })
	);

	function toggleSort() {
		params[sortKey] = params[sortKey] === 'asc' ? 'desc' : 'asc';
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
			<Button variant="secondary" size="sm" icon="leading" on:click={toggleSort}>
				<IconArrowsUpDown />
				Sort {params[sortKey] === 'asc' ? 'A-Z' : 'Z-A'}
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

<script lang="ts">
	import { queryParameters } from 'sveltekit-search-params';

	import { Button } from '$lib/components/ui/button';
	import { cn } from '$lib/utils';

	import IconArrowsUpDown from '~icons/heroicons/arrows-up-down-16-solid';

	import { getSortParamKey, type SortContext, getSortParamsConfig } from '$lib/util/sort';

	let {
		class: className,
		showSort = false,
		context = 'drift'
	}: {
		showCluster?: boolean;
		class?: string;
		showSort?: boolean;
		context?: SortContext;
	} = $props();

	const sortKey = $derived(getSortParamKey(context));
	const params = $derived(
		queryParameters(getSortParamsConfig(context), { pushHistory: false, showDefaults: false })
	);

	function toggleSort() {
		params[sortKey] = params[sortKey] === 'asc' ? 'desc' : 'asc';
	}
</script>

<div class={cn('inline-flex flex-wrap gap-3', className)}>
	{#if showSort}
		<Button variant="secondary" size="sm" icon="leading" on:click={toggleSort}>
			<IconArrowsUpDown />
			Sort {params[sortKey] === 'asc' ? 'A-Z' : 'Z-A'}
		</Button>
	{/if}
</div>

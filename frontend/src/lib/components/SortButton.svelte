<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { queryParameters } from 'sveltekit-search-params';
	import { Button } from 'svelte-ux';

	import IconArrowsUpDown from '~icons/heroicons/arrows-up-down-16-solid';

	import { getSortParamKey, type SortContext, getSortParamsConfig } from '$lib/params/sort';

	let {
		context = 'drift',
		...restProps
	}: {
		context?: SortContext;
	} & ComponentProps<Button> = $props();

	const sortKey = $derived(getSortParamKey(context));
	const params = $derived(
		queryParameters(getSortParamsConfig(context), { pushHistory: false, showDefaults: false })
	);

	function toggleSort() {
		params[sortKey] = params[sortKey] === 'asc' ? 'desc' : 'asc';
	}
</script>

<Button on:click={toggleSort} {...restProps}>
	<IconArrowsUpDown />
	Sort {params[sortKey] === 'asc' ? 'A-Z' : 'Z-A'}
</Button>

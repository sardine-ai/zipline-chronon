<script lang="ts">
	import { queryParameters } from 'sveltekit-search-params';

	import { Button } from '$lib/components/ui/button';

	import IconArrowsUpDown from '~icons/heroicons/arrows-up-down-16-solid';

	import { getSortParamKey, type SortContext, getSortParamsConfig } from '$lib/params/sort';

	let {
		context = 'drift',
		...restProps
	}: {
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

<Button variant="secondary" size="sm" icon="leading" on:click={toggleSort} {...restProps}>
	<IconArrowsUpDown />
	Sort {params[sortKey] === 'asc' ? 'A-Z' : 'Z-A'}
</Button>

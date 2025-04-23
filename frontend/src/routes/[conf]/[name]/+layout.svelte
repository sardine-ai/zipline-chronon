<script lang="ts">
	import { TextField, ToggleGroup, ToggleOption } from 'svelte-ux';
	import { queryParameters } from 'sveltekit-search-params';

	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { entityConfig, getEntityType } from '$lib/types/Entity';

	import PageHeader from '$lib/components/PageHeader.svelte';

	import IconLineage from '~icons/carbon/ibm-cloud-pak-manta-automated-data-lineage';
	import IconArrowLeftFromLine from '~icons/lucide/arrow-left-from-line';
	import IconArrowRightFromLine from '~icons/lucide/arrow-right-from-line';
	import DateRangeField from '$src/lib/components/DateRangeField.svelte';
	import ResetZoomButton from '$src/lib/components/ResetZoomButton.svelte';
	import { isZoomed, resetZoom } from './shared.svelte.js';
	import { getOffsetParamsConfig } from '$src/lib/params/offset';
	import { DEMO_DATE_END, DEMO_DATE_START } from '$src/lib/constants/common.js';

	const { data, children } = $props();

	const params = queryParameters(getOffsetParamsConfig(), {
		pushHistory: false,
		showDefaults: false,
		debounceHistory: 500
	});

	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	let [_, conf, name, tab, primaryNav, secondaryNav] = $derived(page.url.pathname.split('/'));
</script>

<PageHeader
	title={data.conf?.metaData?.name ?? 'Unknown'}
	learnHref={entityConfig[getEntityType(data.conf)].learnHref}
/>

<div class="grid grid-stack">
	<ToggleGroup
		value={tab}
		variant="underline"
		classes={{
			root: '[--color-primary:var(--color-primary-800)] -mx-8',
			options: 'justify-start h-10 px-8',
			option: 'flex items-center gap-2'
		}}
		on:change={(e) => {
			goto(`/${conf}/${name}/${e.detail.value}`);
		}}
	>
		<ToggleOption value="overview">
			<IconLineage />
			Overview
		</ToggleOption>

		<ToggleOption value="offline">
			<IconArrowLeftFromLine />
			Offline
		</ToggleOption>

		<ToggleOption value="online">
			<IconArrowRightFromLine />
			Online
		</ToggleOption>
	</ToggleGroup>

	<div class="flex items-center justify-end gap-3 mb-1">
		{#if isZoomed()}
			<ResetZoomButton onClick={resetZoom} />
		{/if}

		{#if secondaryNav === 'drift'}
			<TextField
				label="Offset:"
				labelPlacement="left"
				type="integer"
				bind:value={params.offset}
				align="right"
				dense
				classes={{
					root: '[--color-primary:var(--color-primary-800)] border focus-within:border-primary',
					container: 'bg-transparent border-0',
					label: 'border-r py-1 px-2',
					input: 'w-6'
				}}
			>
				<div slot="suffix" class="pl-1 text-surface-content/50 text-sm">days</div>
			</TextField>
		{/if}

		<!-- TODO: Setup query param -->
		<DateRangeField value={{ start: DEMO_DATE_START, end: DEMO_DATE_END }} />
	</div>
</div>

{@render children()}

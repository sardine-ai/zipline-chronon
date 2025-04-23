<script lang="ts">
	import { ToggleGroup, ToggleOption } from 'svelte-ux';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';

	import { Separator } from '$src/lib/components/ui/separator';

	import IconArrowRightLeft from '~icons/lucide/arrow-right-left';
	import IconChartLine from '~icons/zipline-ai/chart-line';
	import IconChevronsLeftRightEllipsis from '~icons/lucide/chevrons-left-right-ellipsis';
	import IconTableCells from '~icons/heroicons/table-cells-16-solid';
	import IconUpload from '~icons/lucide/upload';

	const { children } = $props();

	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	let [_, conf, name, tab, primaryNav, secondaryNav] = $derived(page.url.pathname.split('/'));
</script>

<div class="grid grid-cols-[auto_1fr_auto] items-center py-1">
	<ToggleGroup
		value={primaryNav}
		rounded="full"
		classes={{
			root: '[--color-primary:var(--color-primary-800)] my-1',
			options: 'justify-start'
		}}
		on:change={(e) => {
			goto(`/${conf}/${name}/${tab}/${e.detail.value}`);
		}}
	>
		<!-- TODO: Will also check if user has access (internal customer-specific API, etc) -->
		<!-- {#if page.params.conf === 'joins'} -->
		<ToggleOption value="observability">Observability</ToggleOption>
		<!-- {/if} -->

		<ToggleOption value="jobs">Jobs</ToggleOption>
		<ToggleOption value="cost">Cost</ToggleOption>
	</ToggleGroup>

	<!-- TODO: Spacer, possibly add filters -->
	<div></div>

	<ToggleGroup
		value={secondaryNav}
		variant="default"
		gap="px"
		classes={{
			label:
				'bg-secondary text-secondary-foreground [&.selected]:text-primary-foreground hover:text-secondary-foreground hover:bg-secondary/80',
			indicator: 'bg-primary',
			option: 'flex gap-2 items-center'
		}}
		on:change={(e) => {
			goto(`/${conf}/${name}/${tab}/${primaryNav}/${e.detail.value}`);
		}}
	>
		{#if primaryNav === 'observability'}
			<ToggleOption value="distributions">
				<IconTableCells />
				<span>Distributions</span>
			</ToggleOption>

			<ToggleOption value="drift">
				<IconChartLine class="size-4" />
				<span>Drift</span>
			</ToggleOption>

			<ToggleOption value="consistency">
				<IconArrowRightLeft class="size-4" />
				<span>Consistency</span>
			</ToggleOption>
		{:else if primaryNav === 'jobs'}
			<ToggleOption value="streaming">
				<IconChevronsLeftRightEllipsis class="size-4" />
				Streaming
			</ToggleOption>
			<ToggleOption value="uploads">
				<IconUpload class="size-4" />
				Upload
			</ToggleOption>
		{/if}
	</ToggleGroup>
</div>

<Separator fullWidthExtend wide />

{@render children()}

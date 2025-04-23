<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { slide } from 'svelte/transition';
	import { Button, Menu, MenuItem, Toggle } from 'svelte-ux';
	import type { DomainType } from 'layerchart/utils/scales.svelte';

	import type {
		IJobTrackerResponseArgs,
		ILineageResponseArgs,
		INodeKeyArgs
	} from '$src/lib/types/codegen';
	import { getEntityConfig, getEntityLabel } from '$src/lib/types/Entity';

	import IconChevronDown from '~icons/heroicons/chevron-down-16-solid';
	import IconEllipsisVertical from '~icons/heroicons/ellipsis-vertical-20-solid';
	import IconArrowRight from '~icons/heroicons/arrow-right';
	import NodeJobsChart from './NodeJobsChart.svelte';
	import Self from './NodeJobsRow.svelte';

	let {
		node,
		jobTrackerByNodeKeyName,
		lineage,
		depth = 0,
		xDomain = null,
		onBrushEnd
	}: {
		node: INodeKeyArgs;
		jobTrackerByNodeKeyName: Map<string, IJobTrackerResponseArgs>;
		lineage: ILineageResponseArgs;
		depth?: number;
		xDomain?: DomainType;
		onBrushEnd?: ComponentProps<typeof NodeJobsChart>['onBrushEnd'];
	} = $props();

	const nodeInfo = lineage.nodeGraph?.infoMap?.get(node);
	const connections = lineage.nodeGraph?.connections?.get(node);

	const config = nodeInfo?.conf ? getEntityConfig(nodeInfo.conf) : null;
	const Icon = config?.icon;
	const nodeLabel = $derived(getEntityLabel(node.name));

	let isExpanded = $state(depth === 0);
	let canExpand = $derived(connections?.parents?.length ?? 0 > 0);
</script>

<div class="grid grid-cols-[320px_1fr] gap-2 pb-2">
	<button
		class="flex items-center justify-between gap-2 bg-neutral-50 dark:bg-neutral-300 border border-neutral-400 rounded-md py-1"
		style:margin-left={`calc(${depth * 0.75}rem)`}
		onclick={() => (isExpanded = !isExpanded)}
	>
		<div class="flex items-center min-w-0 flex-1">
			<div class="w-4">
				{#if canExpand}
					<IconChevronDown
						class="size-4 transition-transform duration-200 text-neutral-900 shrink-0 {isExpanded
							? ''
							: '-rotate-90'}"
					/>
				{/if}
			</div>
			{#if Icon}
				<!-- <div
					style:--color={config.color}
					class="bg-[hsl(var(--color)/5%)] text-[hsl(var(--color))] w-4 h-4 rounded flex items-center justify-center ml-1 shrink-0"
				>
					<Icon />
				</div> -->
				<div
					style:--color={config.color}
					class="bg-[hsl(var(--color)/5%)] border border-[hsl(var(--color))] text-[hsl(var(--color))] size-7 rounded flex items-center justify-center"
				>
					<Icon class="size-4" />
				</div>
			{/if}

			<div class="truncate pl-3 text-left">
				<div class="text-xs text-surface-content/50">
					{nodeLabel.secondary}
				</div>
				<div class="text-sm text-surface-content">
					{nodeLabel.primary}
				</div>
			</div>
		</div>

		{#if config}
			<Toggle let:on={open} let:toggle let:toggleOff>
				<Button
					variant="outline"
					size="sm"
					class="border-neutral-500 mr-2"
					on:click={(e) => {
						e.stopPropagation();
						toggle();
					}}
				>
					<IconEllipsisVertical class="text-neutral-900" />

					<Menu {open} on:close={toggleOff} placement="bottom-start">
						<MenuItem href="{config.path}/{node.name}">
							Open <IconArrowRight class="ml-2" />
						</MenuItem>
					</Menu>
				</Button>
			</Toggle>
		{/if}
	</button>

	<NodeJobsChart jobTracker={jobTrackerByNodeKeyName.get(node.name!)!} {xDomain} {onBrushEnd} />
</div>

{#if isExpanded}
	<div transition:slide>
		{#each connections?.parents ?? [] as parent}
			<Self
				node={parent}
				{jobTrackerByNodeKeyName}
				{lineage}
				depth={depth + 1}
				{xDomain}
				{onBrushEnd}
			/>
		{/each}
	</div>
{/if}

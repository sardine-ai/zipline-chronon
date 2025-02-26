<script lang="ts">
	import IconChevronDown from '~icons/heroicons/chevron-down-16-solid';
	import IconEllipsisVertical from '~icons/heroicons/ellipsis-vertical-20-solid';
	import type { Writable } from 'svelte/store';
	import CellDivider from '$lib/components/CellDivider.svelte';
	import {
		DropdownMenu,
		DropdownMenuContent,
		DropdownMenuItem,
		DropdownMenuTrigger
	} from '$lib/components/ui/dropdown-menu';
	import { Button } from '$lib/components/ui/button';
	import IconArrowRight from '~icons/heroicons/arrow-right';
	import { page } from '$app/state';
	import { getEntityConfig, type EntityData } from '../types/Entity/Entity';

	let {
		isExpanded,
		canExpand,
		depth,
		name,
		conf
	}: {
		isExpanded: Writable<boolean>;
		canExpand: Writable<boolean>;
		depth: number;
		name: string;
		conf: EntityData;
	} = $props();

	const config = getEntityConfig(conf);

	const Icon = config?.icon;

	function isNodePage() {
		return page.url.pathname === `${config.path}/${name}/job-tracking`;
	}
</script>

<div class="flex items-center justify-center h-full relative">
	<CellDivider />
	<button
		class="flex items-center justify-between bg-neutral-300 border border-neutral-400 rounded-md p-[5px] pl-0 w-full mr-[18px] max-w-[440px] max-h-[32px]"
		style:margin-left={`calc(${depth * 0.75}rem)`}
		onclick={() => ($isExpanded = !$isExpanded)}
		title={name}
	>
		<div class="flex items-center min-w-0 flex-1">
			<div class="w-4">
				{#if $canExpand}
					<IconChevronDown
						class="size-4 transition-transform duration-200 text-neutral-900 flex-shrink-0 {$isExpanded
							? ''
							: '-rotate-90'}"
					/>
				{/if}
			</div>
			{#if Icon}
				<div
					style:--color={config.color}
					class="bg-[hsl(var(--color)/5%)] text-[hsl(var(--color))] w-4 h-4 rounded flex items-center justify-center ml-2 flex-shrink-0"
				>
					<Icon />
				</div>
			{/if}
			<span class="truncate pl-2 text-neutral-900">
				{name}
			</span>
		</div>
		{#if config && !isNodePage()}
			<DropdownMenu>
				<DropdownMenuTrigger class="flex items-center ml-2">
					<Button
						variant="outline"
						size="icon-small"
						onclick={(e: MouseEvent) => e.stopPropagation()}
					>
						<IconEllipsisVertical class="text-neutral-900" />
					</Button>
				</DropdownMenuTrigger>
				<DropdownMenuContent align="start" class="min-w-0 w-fit">
					<DropdownMenuItem href="{config.path}/{name}">
						Open<IconArrowRight class="ml-2" />
					</DropdownMenuItem>
				</DropdownMenuContent>
			</DropdownMenu>
		{/if}
	</button>
</div>

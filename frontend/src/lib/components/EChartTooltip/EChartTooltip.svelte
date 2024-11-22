<script lang="ts">
	import { isMacOS } from '$lib/util/browser';
	import ScrollArea from '$lib/components/ui/scroll-area/scroll-area.svelte';
	import { flyAndScale } from '$lib/utils.js';
	import { formatDate, formatValue } from '$lib/util/format';
	import { createEventDispatcher } from 'svelte';
	import Button from '$lib/components/ui/button/button.svelte';
	import { Separator } from '$lib/components/ui/separator/';
	import Badge from '$lib/components/ui/badge/badge.svelte';

	let {
		visible,
		xValue,
		series,
		clickable = false,
		xAxisCategories = undefined
	}: {
		visible: boolean;
		xValue: number | null;
		series: Array<{
			name: string | undefined;
			value: number;
			color: string;
		}>;
		clickable?: boolean;
		xAxisCategories?: string[];
	} = $props();

	const tooltipHeight = '300px';
	const dispatch = createEventDispatcher();

	function handleSeriesClick(item: (typeof series)[number]) {
		dispatch('click', {
			componentType: 'series',
			data: [xValue, item.value],
			seriesName: item.name
		});
	}

	function getTooltipTitle(xValue: number | null, xAxisCategories?: string[]): string {
		if (xValue === null) return '';

		if (xAxisCategories && typeof xValue === 'number' && Number.isInteger(xValue)) {
			return xAxisCategories[xValue];
		}

		return formatDate(xValue);
	}
</script>

<div class="flex">
	<div class="w-0" style:height={tooltipHeight}></div>
	<div class="max-w-[280px] text-small">
		{#if xValue !== null && visible}
			<div
				class="bg-neutral-200 border border-neutral-400 rounded-md shadow-lg {`max-h-[${tooltipHeight}]`} flex flex-col"
				transition:flyAndScale={{ duration: 200 }}
			>
				<div class="text-neutral-700 bg-neutral-300 w-full px-3 py-[4px]">
					{getTooltipTitle(xValue, xAxisCategories)}
				</div>
				<ScrollArea class="flex flex-col" orientation="both">
					<div class="flex flex-col">
						{#each series as item}
							<Button
								variant="ghost"
								class="px-3 text-small text-neutral-800 text-left justify-between w-full {!clickable &&
									'pointer-events-none'}"
								on:click={() => handleSeriesClick(item)}
							>
								{#if series.length > 1}
									<div class="flex items-center gap-2">
										{#if item.color}
											<div class="w-2 h-2 rounded-full" style:background-color={item.color}></div>
										{/if}
										{#if item.name}
											<span class="mr-5">{item.name}</span>
										{/if}
									</div>
								{/if}
								<span class="text-foreground">{formatValue(item.value)}</span>
							</Button>
						{/each}
					</div>
				</ScrollArea>
				<Separator />
				<div class="text-foreground text-xs px-3 my-2">
					<Badge variant="key">{isMacOS() ? '⌘' : 'Ctrl'}</Badge> to lock tooltip
				</div>
			</div>
		{/if}
	</div>
</div>

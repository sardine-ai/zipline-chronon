<script lang="ts">
	import { isMacOS } from '$lib/util/browser';
	import ScrollArea from '$lib/components/ui/scroll-area/scroll-area.svelte';
	import { flyAndScale } from '$lib/utils.js';
	import { formatDate, formatValue } from '$lib/util/format';
	import { createEventDispatcher } from 'svelte';
	import Button from '$lib/components/ui/button/button.svelte';
	import { Separator } from '$lib/components/ui/separator/';
	import Badge from '$lib/components/ui/badge/badge.svelte';
	import { handleChartHighlight } from '$lib/util/chart';
	import type { EChartsType } from 'echarts';

	let {
		visible,
		xValue,
		series,
		clickable = false,
		xAxisCategories = undefined,
		chart
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
		chart: EChartsType | null;
	} = $props();

	const tooltipHeight = '300px';
	const dispatch = createEventDispatcher();

	function handleSeriesClick(item: (typeof series)[number]) {
		if (!clickable) return;
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

	function handleMouseEnter(seriesName: string) {
		handleChartHighlight(chart, seriesName, 'highlight');
	}

	function handleMouseLeave(seriesName: string) {
		handleChartHighlight(chart, seriesName, 'downplay');
	}
</script>

<div class="flex relative">
	<div class="absolute pointer-events-none w-1" style:height={tooltipHeight}></div>
	{#if xValue !== null && visible}
		<div
			class="absolute text-small bg-neutral-200 border border-neutral-400 rounded-md shadow-lg flex flex-col whitespace-nowrap"
			style:max-height={tooltipHeight}
			transition:flyAndScale={{ duration: 200 }}
		>
			<div class="text-neutral-700 bg-neutral-300 w-full px-3 py-1">
				{getTooltipTitle(xValue, xAxisCategories)}
			</div>
			<ScrollArea class="flex flex-col" orientation="both">
				<div class="flex flex-col">
					{#each series as item}
						<Button
							variant="ghost"
							class="px-3 text-small text-neutral-800 text-left justify-between w-full cursor-{clickable
								? 'pointer'
								: 'default'}"
							on:click={() => handleSeriesClick(item)}
							on:mouseenter={() => handleMouseEnter(item.name ?? '')}
							on:mouseleave={() => handleMouseLeave(item.name ?? '')}
						>
							<div class="flex items-center gap-2">
								{#if item.color}
									<div class="w-2 h-2 rounded-full" style:background-color={item.color}></div>
								{/if}
								{#if item.name}
									<span class="mr-5">{item.name}</span>
								{/if}
							</div>
							<span class="text-foreground">{formatValue(item.value)}</span>
						</Button>
					{/each}
				</div>
			</ScrollArea>
			<Separator />
			<div class="text-foreground text-xs px-3 py-2">
				<Badge variant="key">{isMacOS() ? '⌘' : 'Ctrl'}</Badge> to lock tooltip
			</div>
		</div>
	{/if}
</div>

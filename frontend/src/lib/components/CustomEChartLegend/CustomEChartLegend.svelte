<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import type { EChartsType } from 'echarts';
	import { Icon, ChevronDown, ChevronUp } from 'svelte-hero-icons';
	import { getSeriesColor, handleChartHighlight } from '$lib/util/chart';

	type LegendItem = { feature: string };
	type Props = {
		groupName: string;
		items: Array<LegendItem>;
		chart: EChartsType;
	};

	let { groupName, items, chart }: Props = $props();

	let hiddenSeries: { [key: string]: Set<string> } = $state({});
	let isExpanded = $state(false);
	let itemsContainer: HTMLDivElement;
	let containerHeight = $state(0);
	let hasOverflow = $state(false);
	let containerHeightLine = 24;

	function toggleSeries(seriesName: string) {
		const groupSet = hiddenSeries[groupName] || new Set();

		if (groupSet.has(seriesName)) {
			groupSet.delete(seriesName);
		} else {
			groupSet.add(seriesName);
			chart.dispatchAction({
				type: 'downplay',
				seriesName
			});
		}

		hiddenSeries = {
			...hiddenSeries,
			[groupName]: groupSet
		};

		chart.setOption({ legend: { show: false } });
		chart.dispatchAction({
			type: 'legendToggleSelect',
			name: seriesName
		});
	}

	function checkOverflow() {
		if (!itemsContainer) return;
		const hasVerticalOverflow = itemsContainer.scrollHeight > itemsContainer.clientHeight;
		const hasHorizontalOverflow = itemsContainer.scrollWidth > itemsContainer.clientWidth;
		hasOverflow = hasVerticalOverflow || hasHorizontalOverflow;
	}

	$effect(() => {
		if (!itemsContainer) return;

		const resizeObserver = new ResizeObserver(() => {
			checkOverflow();
			containerHeight = itemsContainer.scrollHeight;
		});

		resizeObserver.observe(itemsContainer);
		checkOverflow();
		containerHeight = itemsContainer.scrollHeight;

		return () => resizeObserver.disconnect();
	});

	function handleMouseEnter(seriesName: string) {
		handleChartHighlight(chart, seriesName, 'highlight');
	}

	function handleMouseLeave(seriesName: string) {
		handleChartHighlight(chart, seriesName, 'downplay');
	}
</script>

<div class="relative mt-5">
	<div class="flex">
		<div
			bind:this={itemsContainer}
			class={`flex flex-wrap gap-x-4 gap-y-2 flex-1 transition-all duration-150 ease-in-out ${!isExpanded ? 'overflow-hidden' : ''}`}
			style="height: {isExpanded ? containerHeight + 'px' : containerHeightLine + 'px'};"
		>
			{#each items as { feature } (feature)}
				{@const isHidden = hiddenSeries[groupName]?.has(feature)}
				{@const color = getSeriesColor(chart, feature)}

				<Button
					class="legend-btn gap-x-2"
					variant="ghost"
					on:click={() => toggleSeries(feature)}
					title={feature}
					on:mouseenter={() => handleMouseEnter(feature)}
					on:mouseleave={() => handleMouseLeave(feature)}
				>
					<div
						class="w-2 h-2 rounded-full flex-shrink-0"
						style="background-color: {isHidden ? 'hsl(var(--neutral-700))' : color}"
					></div>
					<div
						class="{isHidden
							? 'text-neutral-700'
							: ''} overflow-hidden text-ellipsis whitespace-nowrap"
					>
						<span>{feature}</span>
					</div>
				</Button>
			{/each}
		</div>

		{#if hasOverflow || isExpanded || containerHeight > containerHeightLine}
			<Button variant="ghost" class="legend-btn" on:click={() => (isExpanded = !isExpanded)}>
				<div class="transition-opacity duration-150" class:opacity-0={isExpanded}>
					view all ({items.length})
				</div>
				<div class="transition-opacity duration-150 absolute" class:opacity-0={!isExpanded}>
					collapse
				</div>
				<Icon src={isExpanded ? ChevronUp : ChevronDown} micro size="16" class="ml-2" />
			</Button>
		{/if}
	</div>
</div>

<style lang="postcss">
	:global(.legend-btn) {
		@apply flex items-center hover:bg-transparent font-normal h-6 p-0 text-sm !important;
	}
</style>

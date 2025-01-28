<script lang="ts">
	import ResetZoomButton from '$lib/components/ResetZoomButton.svelte';
	import MetricTypeToggle from '$lib/components/MetricTypeToggle.svelte';
	import DateRangeSelector from '$lib/components/DateRangeSelector.svelte';
	import ActionButtons from '$lib/components/ActionButtons.svelte';
	import * as Alert from '$lib/components/ui/alert/index.js';
	import { formatDate } from '$lib/util/format';
	import type { SortContext } from '$lib/util/sort';

	let {
		isZoomed = false,
		onResetZoom,
		isUsingFallbackDates = false,
		dateRange = { startTimestamp: 0, endTimestamp: 0 },
		showActionButtons = false,
		showCluster = false,
		showSort = false,
		context
	}: {
		isZoomed: boolean;
		onResetZoom: () => void;
		isUsingFallbackDates?: boolean;
		dateRange?: { startTimestamp: number; endTimestamp: number };
		showActionButtons?: boolean;
		showCluster?: boolean;
		showSort?: boolean;
		context?: SortContext;
	} = $props();
</script>

<div class="space-y-4">
	{#if isUsingFallbackDates}
		<div class="w-fit">
			<Alert.Root variant="warning">
				<Alert.Description>
					No data for that date range. Showing data between {formatDate(dateRange.startTimestamp)} and
					{formatDate(dateRange.endTimestamp)}</Alert.Description
				>
			</Alert.Root>
		</div>
	{/if}

	<div class="flex items-center space-x-6">
		{#if isZoomed}
			<ResetZoomButton onClick={onResetZoom} />
		{/if}
		<DateRangeSelector />
		{#if context === 'drift'}
			<MetricTypeToggle />
		{/if}
	</div>

	{#if showActionButtons}
		<div>
			<ActionButtons {showCluster} {showSort} {context} />
		</div>
	{/if}
</div>

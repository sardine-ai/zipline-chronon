<script lang="ts">
	import ResetZoomButton from '$lib/components/ResetZoomButton.svelte';
	import DriftMetricToggle from '$lib/components/DriftMetricToggle.svelte';
	import DateRangeSelector from '$lib/components/DateRangeSelector.svelte';
	import ActionButtons from '$lib/components/ActionButtons.svelte';
	import type { SortContext } from '$lib/util/sort';
	import { fromAbsolute, getLocalTimeZone } from '@internationalized/date';

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
	<div class="flex items-center space-x-6">
		{#if isZoomed}
			<ResetZoomButton onClick={onResetZoom} />
		{/if}
		<DateRangeSelector
			fallbackDateRange={isUsingFallbackDates
				? {
						start: fromAbsolute(dateRange.startTimestamp, getLocalTimeZone()),
						end: fromAbsolute(dateRange.endTimestamp, getLocalTimeZone())
					}
				: undefined}
		/>
		{#if context === 'drift'}
			<DriftMetricToggle />
		{/if}

		{#if showActionButtons}
			<ActionButtons {showCluster} {showSort} {context} />
		{/if}
	</div>
</div>

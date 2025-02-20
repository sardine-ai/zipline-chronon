<script lang="ts">
	import { untrack } from 'svelte';
	import { queryParameters } from 'sveltekit-search-params';
	import type { DateRange } from 'bits-ui';
	import { DateFormatter, getLocalTimeZone, fromAbsolute } from '@internationalized/date';

	import {
		DATE_RANGE_PARAM,
		DATE_RANGES,
		CUSTOM,
		DATE_RANGE_START_PARAM,
		DATE_RANGE_END_PARAM,
		getDateRangeByValue
	} from '$lib/constants/date-ranges';
	import { Button } from '$lib/components/ui/button';
	import { Popover, PopoverContent, PopoverTrigger } from '$lib/components/ui/popover';
	import IconCalendarDateRange from '~icons/heroicons/calendar-date-range-16-solid';

	import { cn } from '$lib/utils';
	import { RangeCalendar } from '$lib/components/ui/range-calendar/index';
	import { getDateRangeParamsConfig } from '$lib/util/date-ranges';
	import IconInformationCircle from '~icons/heroicons/information-circle';
	import { Tooltip, TooltipContent, TooltipTrigger } from './ui/tooltip';

	const { fallbackDateRange }: { fallbackDateRange?: DateRange } = $props();

	const params = queryParameters(getDateRangeParamsConfig(), {
		pushHistory: false,
		showDefaults: false
	});

	const df = new DateFormatter('en-US', {
		dateStyle: 'medium'
	});

	let selectDateRange = $derived(getDateRangeByValue(params[DATE_RANGE_PARAM]));

	let calendarDateRange: DateRange | undefined = $state({
		start: undefined,
		end: undefined
	});

	// Update `calendarDateRange` when `selectDateRange` (preset selection) changes
	$effect(() => {
		const selectedRange = selectDateRange?.getRange();
		const isCustomPreset = selectDateRange?.value === CUSTOM;
		untrack(() => {
			const startRange = isCustomPreset ? params[DATE_RANGE_START_PARAM] : selectedRange?.[0];
			const endRange = isCustomPreset ? params[DATE_RANGE_END_PARAM] : selectedRange?.[1];

			if (startRange && endRange) {
				calendarDateRange = {
					start: fromAbsolute(startRange, getLocalTimeZone()),
					end: fromAbsolute(endRange, getLocalTimeZone())
				};
			}
		});
	});

	let dateRangePopoverOpen = $state(false);
	let calendarDateRangePopoverOpen = $state(false);

	function handleDateRangeSelect(value: string) {
		updateURLParams(value);
		dateRangePopoverOpen = false;
	}

	function handleCalendarChange(newSelectedDateRange: DateRange | undefined) {
		if (newSelectedDateRange && newSelectedDateRange.start && newSelectedDateRange.end) {
			const startDate = newSelectedDateRange.start.toDate(getLocalTimeZone()).getTime();
			const endDate = newSelectedDateRange.end.toDate(getLocalTimeZone()).getTime();
			updateURLParams(CUSTOM, startDate, endDate);
			calendarDateRangePopoverOpen = false;
		}
	}

	function updateURLParams(range: string, start?: number, end?: number) {
		params[DATE_RANGE_PARAM] = range;

		if (range === CUSTOM && start && end) {
			params[DATE_RANGE_START_PARAM] = start;
			params[DATE_RANGE_END_PARAM] = end;
		} else {
			params[DATE_RANGE_START_PARAM] = null;
			params[DATE_RANGE_END_PARAM] = null;
		}
	}

	function getNonCustomDateRanges() {
		return DATE_RANGES.filter((range) => range.value !== CUSTOM);
	}
</script>

<Tooltip>
	<TooltipTrigger>
		<div class="flex items-center">
			<Popover bind:open={dateRangePopoverOpen}>
				<PopoverTrigger asChild let:builder>
					<Button
						variant="outline"
						size="sm"
						class={cn(
							'rounded-r-none',
							dateRangePopoverOpen && 'border border-primary-800',
							'_border-warning-foreground'
						)}
						builders={[builder]}
					>
						<span>{selectDateRange?.label || 'Select range'}</span>
						<span class="border-input border-l mx-2 h-full _border-warning-foreground"></span>
						<span>
							{#if calendarDateRangePopoverOpen || (calendarDateRange?.start == null && fallbackDateRange?.start == null)}
								Pick a date
							{:else if fallbackDateRange?.start}
								{#if fallbackDateRange.end}
									{df.format(fallbackDateRange.start.toDate(getLocalTimeZone()))} - {df.format(
										fallbackDateRange.end.toDate(getLocalTimeZone())
									)}
								{:else}
									{df.format(fallbackDateRange.start.toDate(getLocalTimeZone()))}
								{/if}
							{:else if calendarDateRange?.start}
								{#if calendarDateRange.end}
									{df.format(calendarDateRange.start.toDate(getLocalTimeZone()))} - {df.format(
										calendarDateRange.end.toDate(getLocalTimeZone())
									)}
								{:else}
									{df.format(calendarDateRange.start.toDate(getLocalTimeZone()))}
								{/if}
							{/if}
						</span>
						{#if fallbackDateRange}
							<IconInformationCircle class="ml-3 text-warning-foreground" />
						{/if}
					</Button>
				</PopoverTrigger>
				<PopoverContent class="w-[200px] p-0" align="start">
					{#each getNonCustomDateRanges() as range}
						<Button
							variant="ghost"
							class="w-full justify-start"
							onclick={() => handleDateRangeSelect(range.value)}
						>
							{range.label}
						</Button>
					{/each}
				</PopoverContent>
			</Popover>

			<Popover bind:open={calendarDateRangePopoverOpen} openFocus>
				<PopoverTrigger asChild let:builder>
					<Button
						variant="outline"
						size="sm"
						class={cn(
							'rounded-l-none border-l-transparent _border-warning-foreground',
							calendarDateRangePopoverOpen &&
								'border border-primary-800 bg-primary hover:bg-primary'
						)}
						builders={[builder]}
					>
						<IconCalendarDateRange />
					</Button>
				</PopoverTrigger>
				<PopoverContent class="w-auto p-0" align="start">
					<RangeCalendar
						bind:value={calendarDateRange}
						numberOfMonths={1}
						onValueChange={handleCalendarChange}
						weekdayFormat="narrow"
					/>
				</PopoverContent>
			</Popover>
		</div>
	</TooltipTrigger>

	{#if fallbackDateRange}
		<TooltipContent side="bottom">
			{#if calendarDateRange?.start && calendarDateRange?.end}
				No data for
				<span class="font-bold"
					>{df.format(calendarDateRange.start.toDate(getLocalTimeZone()))} - {df.format(
						calendarDateRange.end.toDate(getLocalTimeZone())
					)}</span
				>. Showing fallback data
			{/if}
		</TooltipContent>
	{/if}
</Tooltip>

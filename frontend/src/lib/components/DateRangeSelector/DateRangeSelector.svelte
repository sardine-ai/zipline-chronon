<script lang="ts">
	import {
		DATE_RANGE_PARAM,
		DATE_RANGES,
		CUSTOM,
		DATE_RANGE_START_PARAM,
		DATE_RANGE_END_PARAM,
		type DateRangeOption,
		getDateRangeByValue
	} from '$lib/constants/date-ranges';
	import { Button } from '$lib/components/ui/button';
	import { Popover, PopoverContent, PopoverTrigger } from '$lib/components/ui/popover';
	import { Icon, CalendarDateRange } from 'svelte-hero-icons';
	import type { DateRange } from 'bits-ui';
	import { DateFormatter, getLocalTimeZone, fromAbsolute } from '@internationalized/date';
	import { cn } from '$lib/utils';
	import { RangeCalendar } from '$lib/components/ui/range-calendar/index';
	import { goto } from '$app/navigation';
	import { page } from '$app/stores';
	import { untrack } from 'svelte';
	import { parseDateRangeParams } from '$lib/util/date-ranges';

	const df = new DateFormatter('en-US', {
		dateStyle: 'medium'
	});

	let selectDateRange: DateRangeOption | undefined = $state(undefined);
	let calendarDateRange: DateRange | undefined = $state({
		start: undefined,
		end: undefined
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
			updateURLParams(CUSTOM, startDate.toString(), endDate.toString());
			calendarDateRangePopoverOpen = false;
		}
	}

	function updateURLParams(range: string, start?: string, end?: string) {
		const url = new URL($page.url);
		url.searchParams.set(DATE_RANGE_PARAM, range);

		if (range === CUSTOM && start && end) {
			url.searchParams.set(DATE_RANGE_START_PARAM, start);
			url.searchParams.set(DATE_RANGE_END_PARAM, end);
		} else {
			url.searchParams.delete(DATE_RANGE_START_PARAM);
			url.searchParams.delete(DATE_RANGE_END_PARAM);
		}

		goto(url.toString(), { replaceState: true });
	}

	$effect(() => {
		const url = new URL($page.url);
		untrack(() => {
			const { dateRangeValue, startTimestamp, endTimestamp } = parseDateRangeParams(
				url.searchParams
			);
			selectDateRange = getDateRangeByValue(dateRangeValue);
			calendarDateRange = {
				start: fromAbsolute(Number(startTimestamp), getLocalTimeZone()),
				end: fromAbsolute(Number(endTimestamp), getLocalTimeZone())
			};
		});
	});

	function getNonCustomDateRanges() {
		return DATE_RANGES.filter((range) => range.value !== CUSTOM);
	}
</script>

<div class="flex items-center">
	<Popover bind:open={dateRangePopoverOpen}>
		<PopoverTrigger asChild let:builder>
			<Button
				variant="outline"
				size="sm"
				class={cn('rounded-r-none', dateRangePopoverOpen && 'border border-primary-800')}
				builders={[builder]}
			>
				<span>{selectDateRange?.label || 'Select range'}</span>
				<span class="border-input border-l mx-2 h-full"></span>
				<span>
					{#if calendarDateRange && calendarDateRange.start}
						{#if calendarDateRange.end}
							{df.format(calendarDateRange.start.toDate(getLocalTimeZone()))} - {df.format(
								calendarDateRange.end.toDate(getLocalTimeZone())
							)}
						{:else}
							{df.format(calendarDateRange.start.toDate(getLocalTimeZone()))}
						{/if}
					{:else}
						Pick a date
					{/if}
				</span>
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
					'rounded-l-none border-l-transparent',
					calendarDateRangePopoverOpen && 'border border-primary-800 bg-primary hover:bg-primary'
				)}
				builders={[builder]}
			>
				<Icon src={CalendarDateRange} micro size="16" />
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

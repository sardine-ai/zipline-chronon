<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { DateRangeField } from 'svelte-ux';
	import { PeriodType } from '@layerstack/utils';
	import { subDays, subHours, subMonths } from 'date-fns';

	import IconCalendarDateRange from '~icons/heroicons/calendar-date-range-16-solid';

	let {
		value,
		...restProps
	}: { value?: { start: Date; end: Date } } & Omit<ComponentProps<DateRangeField>, 'value'> =
		$props();

	const now = new Date();
</script>

<DateRangeField
	label=""
	value={value ? { from: value.start, to: value.end, periodType: PeriodType.Day } : undefined}
	periodTypes={[PeriodType.Day]}
	getPeriodTypePresets={() => []}
	quickPresets={[
		{
			label: 'Last 24 hours',
			value: { from: subHours(now, 24), to: now, periodType: PeriodType.Day }
		},
		{
			label: 'Last 7 days',
			value: { from: subDays(now, 7), to: now, periodType: PeriodType.Day }
		},
		{
			label: 'Last 30 days',
			value: {
				from: subDays(now, 30),
				to: now,
				periodType: PeriodType.Day
			}
		},
		{
			label: 'Last 6 months',
			value: {
				from: subMonths(now, 6),
				to: now,
				periodType: PeriodType.Day
			}
		}
	]}
	dense
	classes={{
		field: {
			container: 'bg-transparent'
		}
	}}
	{...restProps}
>
	{#snippet prepend()}
		<IconCalendarDateRange class="text-surface-content/50 size-4 mr-2" />
	{/snippet}
</DateRangeField>

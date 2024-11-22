export const DATE_RANGE_PARAM = 'date-range';
export const DATE_RANGE_START_PARAM = 'start';
export const DATE_RANGE_END_PARAM = 'end';

export const PAST_1_WEEK = 'past-1-week';
export const CUSTOM = 'custom';

export interface DateRange {
	value: string;
	label: string;
	getRange: () => [number, number];
}

export const DATE_RANGES: DateRange[] = [
	{
		value: 'past-3-hours',
		label: 'Past 3 hours',
		getRange: () => {
			const end = Date.now();
			return [end - 3 * 60 * 60 * 1000, end];
		}
	},
	{
		value: 'past-12-hours',
		label: 'Past 12 hours',
		getRange: () => {
			const end = Date.now();
			return [end - 12 * 60 * 60 * 1000, end];
		}
	},
	{
		value: 'past-1-day',
		label: 'Past 1 day',
		getRange: () => {
			const end = Date.now();
			return [end - 24 * 60 * 60 * 1000, end];
		}
	},
	{
		value: PAST_1_WEEK,
		label: 'Past 1 week',
		getRange: () => {
			const end = Date.now();
			return [end - 7 * 24 * 60 * 60 * 1000, end];
		}
	},
	{
		value: 'past-1-month',
		label: 'Past 1 month',
		getRange: () => {
			const end = Date.now();
			return [end - 30 * 24 * 60 * 60 * 1000, end];
		}
	},
	{
		value: CUSTOM,
		label: 'Custom',
		getRange: () => [Date.now() - 7 * 24 * 60 * 60 * 1000, Date.now()] // Default to last 7 days
	}
] as const;

export type DateRangeOption = (typeof DATE_RANGES)[number];

export function getDateRangeByValue(value: string): DateRange | undefined {
	return DATE_RANGES.find((range) => range.value === value);
}

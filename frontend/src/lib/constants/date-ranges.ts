export const DATE_RANGE_PARAM = 'date-range';
export const DATE_RANGE_START_PARAM = 'start';
export const DATE_RANGE_END_PARAM = 'end';

export const LAST_DAY = 'last-day';
export const LAST_7_DAYS = 'last-7-days';
export const LAST_MONTH = 'last-month';
export const CUSTOM = 'custom';

export const DATE_RANGES = [
	{ value: LAST_DAY, label: 'Last Day' },
	{ value: LAST_7_DAYS, label: 'Last 7 Days' },
	{ value: LAST_MONTH, label: 'Last Month' },
	{ value: CUSTOM, label: 'Custom' }
] as const;

export type DateRangeOption = (typeof DATE_RANGES)[number];

export function getDateRangeByValue(value: string): DateRangeOption | undefined {
	return DATE_RANGES.find((range) => range.value === value);
}

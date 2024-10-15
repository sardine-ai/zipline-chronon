export const DATE_RANGE_PARAM = 'date-range';

export const LAST_DAY = 'last-day';
export const LAST_7_DAYS = 'last-7-days';
export const LAST_MONTH = 'last-month';

export const DATE_RANGES = [
	{ value: LAST_DAY, label: 'Last Day' },
	{ value: LAST_7_DAYS, label: 'Last 7 Days' },
	{ value: LAST_MONTH, label: 'Last Month' }
] as const;

export type DateRangeOption = (typeof DATE_RANGES)[number];

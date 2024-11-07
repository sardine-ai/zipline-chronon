import {
	DATE_RANGE_PARAM,
	DATE_RANGE_START_PARAM,
	DATE_RANGE_END_PARAM,
	LAST_7_DAYS,
	LAST_DAY,
	LAST_MONTH
} from '$lib/constants/date-ranges';

export function getDateRange(range: string): [number, number] {
	const now = new Date();
	const end = now.getTime();
	let start: number;

	switch (range) {
		case LAST_DAY:
			start = end - 24 * 60 * 60 * 1000;
			break;
		case LAST_7_DAYS:
			start = end - 7 * 24 * 60 * 60 * 1000;
			break;
		case LAST_MONTH:
			start = new Date(now.getFullYear(), now.getMonth() - 1, now.getDate()).getTime();
			break;
		default:
			start = end - 7 * 24 * 60 * 60 * 1000; // Default to last 7 days
	}

	return [start, end];
}

export function parseDateRangeParams(searchParams: URLSearchParams) {
	const dateRangeValue = searchParams.get(DATE_RANGE_PARAM);
	const startParam = searchParams.get(DATE_RANGE_START_PARAM);
	const endParam = searchParams.get(DATE_RANGE_END_PARAM);

	let startTimestamp: number;
	let endTimestamp: number;

	if (startParam && endParam) {
		startTimestamp = Number(startParam);
		endTimestamp = Number(endParam);
	} else {
		[startTimestamp, endTimestamp] = getDateRange(dateRangeValue || LAST_7_DAYS);
	}

	return {
		dateRangeValue: dateRangeValue || LAST_7_DAYS,
		startTimestamp,
		endTimestamp
	};
}

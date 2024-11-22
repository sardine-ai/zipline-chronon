import {
	DATE_RANGE_PARAM,
	DATE_RANGE_START_PARAM,
	DATE_RANGE_END_PARAM,
	PAST_1_WEEK,
	getDateRangeByValue
} from '$lib/constants/date-ranges';

export function getDateRange(range: string): [number, number] {
	const dateRange = getDateRangeByValue(range);
	return dateRange ? dateRange.getRange() : getDateRangeByValue(PAST_1_WEEK)!.getRange();
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
		[startTimestamp, endTimestamp] = getDateRange(dateRangeValue || PAST_1_WEEK);
	}

	return {
		dateRangeValue: dateRangeValue || PAST_1_WEEK,
		startTimestamp,
		endTimestamp
	};
}

import { ssp } from 'sveltekit-search-params';

import {
	DATE_RANGE_PARAM,
	DATE_RANGE_START_PARAM,
	DATE_RANGE_END_PARAM,
	PAST_1_WEEK,
	getDateRangeByValue
} from '$lib/constants/date-ranges';
import { getSearchParamValues } from './search-params';

export function getDateRange(range: string): [number, number] {
	const dateRange = getDateRangeByValue(range);
	return dateRange ? dateRange.getRange() : getDateRangeByValue(PAST_1_WEEK)!.getRange();
}

export function getDateRangeParamsConfig() {
	return {
		[DATE_RANGE_PARAM]: ssp.string(PAST_1_WEEK),
		[DATE_RANGE_START_PARAM]: ssp.number(),
		[DATE_RANGE_END_PARAM]: ssp.number()
	};
}

export function parseDateRangeParams(searchParams: URLSearchParams) {
	const paramsConfig = getDateRangeParamsConfig();
	const paramValues = getSearchParamValues(searchParams, paramsConfig);

	if (paramValues[DATE_RANGE_START_PARAM] == null || paramValues[DATE_RANGE_END_PARAM] == null) {
		const [start, end] = getDateRange(paramValues[DATE_RANGE_PARAM] || PAST_1_WEEK);
		paramValues[DATE_RANGE_START_PARAM] = start;
		paramValues[DATE_RANGE_END_PARAM] = end;
	}

	return {
		dateRangeValue: paramValues[DATE_RANGE_PARAM],
		startTimestamp: paramValues[DATE_RANGE_START_PARAM],
		endTimestamp: paramValues[DATE_RANGE_END_PARAM]
	};
}

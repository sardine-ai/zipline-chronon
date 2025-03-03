import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';
import { getSearchParamValues } from './search-params';

export const SORT_DIRECTIONS = ['asc', 'desc'] as const;
export type SortDirection = (typeof SORT_DIRECTIONS)[number];
export type SortContext = 'drift' | 'summary';

export function getSortParamKey(context: SortContext): string {
	return `${context}Sort`;
}

export function getSortParamsConfig(context: SortContext) {
	const sortKey = getSortParamKey(context);
	return {
		[sortKey]: {
			encode: (value) => value,
			decode: (value) =>
				SORT_DIRECTIONS.includes(value as SortDirection) ? (value as SortDirection) : null,
			defaultValue: 'asc'
		} satisfies EncodeAndDecodeOptions<SortDirection>
	};
}

export function getSortDirection(
	searchParams: URLSearchParams,
	context: SortContext
): SortDirection {
	const paramValues = getSearchParamValues(searchParams, getSortParamsConfig(context));
	return paramValues[getSortParamKey(context)];
}

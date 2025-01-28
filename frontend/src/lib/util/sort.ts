import type { FeatureResponse, JoinTimeSeriesResponse } from '$lib/types/Model/Model';
import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';

export const SORT_DIRECTIONS = ['asc', 'desc'] as const;
export type SortDirection = (typeof SORT_DIRECTIONS)[number];
export type SortContext = 'drift' | 'distributions';

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
	const param = searchParams.get(getSortParamKey(context));
	return param === 'desc' ? 'desc' : 'asc';
}

export function sortDrift(
	joinTimeseries: JoinTimeSeriesResponse,
	direction: SortDirection
): JoinTimeSeriesResponse {
	const sorted = { ...joinTimeseries };

	// Sort main groups
	sorted.items = [...joinTimeseries.items].sort((a, b) => {
		const comparison = a.name.localeCompare(b.name);
		return direction === 'asc' ? comparison : -comparison;
	});

	// Sort features within each group
	sorted.items.forEach((group) => {
		group.items.sort((a, b) => a.feature.localeCompare(b.feature));
	});

	return sorted;
}

export function sortDistributions(
	distributions: FeatureResponse[],
	direction: SortDirection
): FeatureResponse[] {
	return [...distributions].sort((a, b) => {
		const comparison = a.feature.localeCompare(b.feature);
		return direction === 'asc' ? comparison : -comparison;
	});
}

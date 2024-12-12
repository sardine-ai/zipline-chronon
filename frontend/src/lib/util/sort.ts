import type { FeatureResponse, JoinTimeSeriesResponse } from '$lib/types/Model/Model';

export type SortDirection = 'asc' | 'desc';
export type SortContext = 'drift' | 'distributions';

export function getSortParamKey(context: SortContext): string {
	return `${context}Sort`;
}

export function getSortDirection(
	searchParams: URLSearchParams,
	context: SortContext
): SortDirection {
	const param = searchParams.get(getSortParamKey(context));
	return param === 'desc' ? 'desc' : 'asc';
}

export function updateContextSort(url: URL, context: SortContext, direction: SortDirection): URL {
	const newUrl = new URL(url);
	newUrl.searchParams.set(getSortParamKey(context), direction);
	return newUrl;
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

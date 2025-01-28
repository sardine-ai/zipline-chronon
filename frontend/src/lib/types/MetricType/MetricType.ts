import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';
import { getSearchParamValues } from '$lib/util/search-params';

export const METRIC_TYPES = ['jsd', 'hellinger', 'psi'] as const;
export type MetricType = (typeof METRIC_TYPES)[number];

export const DEFAULT_METRIC_TYPE: MetricType = 'psi';

export const METRIC_LABELS: Record<MetricType, string> = {
	jsd: 'JSD',
	hellinger: 'Hellinger',
	psi: 'PSI'
};

export const METRIC_SCALES: Record<MetricType, { min: number; max: number }> = {
	jsd: { min: 0, max: 1 },
	hellinger: { min: 0, max: 1 },
	psi: { min: 0, max: 25 }
};

export function getMetricTypeParamsConfig() {
	return {
		metric: {
			encode: (value) => value,
			decode: (value) =>
				METRIC_TYPES.includes(value as MetricType) ? (value as MetricType) : null,
			defaultValue: DEFAULT_METRIC_TYPE
		} satisfies EncodeAndDecodeOptions<MetricType>
	};
}

export function getMetricTypeFromParams(searchParams: URLSearchParams) {
	const paramsConfig = getMetricTypeParamsConfig();
	return getSearchParamValues(searchParams, paramsConfig).metric;
}

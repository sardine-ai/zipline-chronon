import type { EncodeAndDecodeOptions } from 'sveltekit-search-params/sveltekit-search-params';

import { getSearchParamValues } from './search-params';
import { DriftMetric } from '$lib/types/codegen';

export const DEFAULT_DRIFT_METRIC: DriftMetric = DriftMetric.PSI;

export const DRIFT_METRIC_LABELS: Record<DriftMetric, string> = {
	[DriftMetric.JENSEN_SHANNON]: 'JSD',
	[DriftMetric.HELLINGER]: 'Hellinger',
	[DriftMetric.PSI]: 'PSI'
};

export const DRIFT_METRIC_SCALES: Record<DriftMetric, { min: number; max: number }> = {
	[DriftMetric.JENSEN_SHANNON]: { min: 0, max: 1 },
	[DriftMetric.HELLINGER]: { min: 0, max: 1 },
	[DriftMetric.PSI]: { min: 0, max: 25 }
};

export function getDriftMetricParamsConfig() {
	return {
		metric: {
			encode: (value) => value.toString(),
			decode: (value) => (value ? (Number(value) as DriftMetric) : null),
			defaultValue: DEFAULT_DRIFT_METRIC
		} satisfies EncodeAndDecodeOptions<DriftMetric>
	};
}

export function getDriftMetricFromParams(searchParams: URLSearchParams) {
	const paramsConfig = getDriftMetricParamsConfig();
	return getSearchParamValues(searchParams, paramsConfig).metric;
}

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

export function getMetricTypeFromParams(searchParams: URLSearchParams): MetricType {
	const metric = searchParams.get('metric');
	return METRIC_TYPES.includes(metric as MetricType) ? (metric as MetricType) : DEFAULT_METRIC_TYPE;
}

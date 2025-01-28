import type { EChartsType } from 'echarts';

export function handleChartHighlight(
	chart: EChartsType | null,
	seriesName: string,
	type: 'highlight' | 'downplay'
) {
	if (!chart || !seriesName) return;

	// Get the series selected state from legend
	const options = chart.getOption();
	const legendOpt = Array.isArray(options.legend) ? options.legend[0] : options.legend;
	const isSelected = legendOpt?.selected?.[seriesName];

	// Only highlight if the series is selected (visible)
	if (isSelected !== false) {
		chart.dispatchAction({
			type,
			seriesName
		});
	}
}

export function getSeriesColor(chart: EChartsType | null, seriesName: string): string {
	if (!chart) return '#000000';

	const options = chart.getOption();
	if (!options?.series || !options?.color) return '#000000';

	// Find the series index by name
	const seriesIndex = options.series.findIndex((s) => s.name === seriesName);
	if (seriesIndex === -1) return '#000000';

	// Get color using the correct series index
	const colors = options.color as string[];
	return colors[seriesIndex] || '#000000';
}

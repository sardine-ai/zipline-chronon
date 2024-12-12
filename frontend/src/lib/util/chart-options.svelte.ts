import type { EChartOption } from 'echarts';
import merge from 'lodash/merge';
import { getCssColorAsHex } from '$lib/util/colors';

let neutral300 = $state('');
let neutral700 = $state('');

const colorInterval = setInterval(() => {
	const color300 = getCssColorAsHex('--neutral-300');
	const color700 = getCssColorAsHex('--neutral-700');

	if (color300 && color700) {
		neutral300 = color300;
		neutral700 = color700;
		clearInterval(colorInterval);
	}
}, 100);

export function createChartOption(
	customOption: Partial<EChartOption> = {},
	customColors = false
): EChartOption {
	const defaultOption: EChartOption = {
		color: customColors
			? [
					'#E5174B',
					'#E54D4A',
					'#E17545',
					'#E3994C',
					'#DFAF4F',
					'#87BE52',
					'#53B167',
					'#4DA67D',
					'#4EA797',
					'#4491CE',
					'#4592CC',
					'#4172D2',
					'#5B5AD1',
					'#785AD4',
					'#9055D5',
					'#BF50D3',
					'#CB5587'
				]
			: undefined,
		tooltip: {
			trigger: 'axis',
			axisPointer: {
				type: 'line',
				lineStyle: {
					color: neutral700,
					type: 'solid'
				}
			},
			position: 'top',
			confine: true
		},
		xAxis: {
			type: 'time',
			axisLabel: {
				formatter: {
					month: '{MMM} {d}',
					day: '{MMM} {d}'
				} as unknown as string,
				color: neutral700
			},
			splitLine: {
				show: true,
				lineStyle: {
					color: neutral300
				}
			},
			axisLine: {
				lineStyle: {
					color: neutral300
				}
			}
		},
		yAxis: {
			type: 'value',
			axisLabel: {
				formatter: (value: number) => (value % 1 === 0 ? value.toFixed(0) : value.toFixed(1)),
				color: neutral700
			},
			splitLine: {
				show: true,
				lineStyle: {
					color: neutral300
				}
			},
			axisLine: {
				lineStyle: {
					color: neutral300
				}
			}
		},
		grid: {
			top: 5,
			right: 1,
			bottom: 0,
			left: 0,
			containLabel: true
		}
	};

	const baseSeriesStyle = {
		showSymbol: false,
		lineStyle: {
			width: 1
		},
		symbolSize: 7
	};

	if (customOption.series) {
		const series = Array.isArray(customOption.series) ? customOption.series : [customOption.series];
		customOption.series = series.map((s) => merge({}, baseSeriesStyle, s));
	}

	return merge({}, defaultOption, customOption);
}

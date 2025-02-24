import { zip, scaleOrdinal } from 'd3';
import { Int64 } from '@creditkarma/thrift-server-core';
import type { ITileDriftSeriesArgs } from '$src/lib/types/codegen';
import { NULL_VALUE } from '$src/lib/constants/common';
import { colors } from '$lib/components/charts/common';

export function getColumns(data: ITileDriftSeriesArgs[]) {
	return [...new Set(data.map((d) => d.key?.column ?? 'Unknown'))];
}

export function transformSeries(series: ITileDriftSeriesArgs, columns: string[]) {
	const colorScale = scaleOrdinal<string>().domain(columns).range(colors);
	const timestamps = series.timestamps ?? [];
	const column = series.key?.column ?? 'Unknown';
	// `percentileDriftSeries` = numeric column, `histogramDriftSeries` = categorical column
	const values = series.percentileDriftSeries ?? series.histogramDriftSeries ?? [];

	return {
		key: column,
		data: zip<Int64 | number>(
			timestamps.map((ts) => Number(ts)),
			values
		).map(([ts, value]) => ({
			date: new Date(ts as number),
			value: value === NULL_VALUE ? null : value
		})),
		color: colorScale(column)
	};
}

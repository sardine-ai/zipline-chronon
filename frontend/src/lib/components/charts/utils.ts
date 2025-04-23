import { scaleOrdinal } from 'd3';

import type { ITileDriftSeriesArgs, ITileSummarySeriesArgs } from '$src/lib/types/codegen';
import { NULL_VALUE } from '$lib/constants/common';
import { colors } from './common';

/**
 * Transform the tile series data into a LayerChart series
 */
export function transformSeries<T extends ITileSummarySeriesArgs | ITileDriftSeriesArgs>(
	data: T[],
	values: (s: T) => number[]
) {
	const columns = getColumns(data);
	const colorScale = scaleOrdinal<string>().domain(columns).range(colors);

	return data.map((d) => {
		const timestamps = d.timestamps ?? [];
		const column = d.key?.column ?? 'Unknown';
		const _values = values(d);

		return {
			key: column,
			data: timestamps.map((ts, i) => {
				return {
					date: new Date(ts as number),
					value: _values[i] === NULL_VALUE ? null : _values[i]
				};
			}),
			color: colorScale(column)
		};
	});
}

/**
 * Get the unique columns from the data
 */
function getColumns(data: ITileSummarySeriesArgs[] | ITileDriftSeriesArgs[]) {
	return [...new Set(data.map((d) => d.key?.column ?? 'Unknown'))];
}

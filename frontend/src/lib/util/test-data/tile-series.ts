import { randomUniform, randomInt } from 'd3';
import type {
	ITileDriftSeriesArgs,
	ITileSeriesKeyArgs,
	ITileSummarySeriesArgs
} from '../../types/codegen';

export function generateTileSummarySeriesData({
	key,
	startDate,
	endDate,
	points,
	normal,
	anomalies = [],
	totalType = 'integer'
}: {
	key: ITileSeriesKeyArgs;
	startDate: Date;
	endDate: Date;
	points: number;
	normal: {
		total: number[] | null;
		nullRatio: number[];
	};
	anomalies?: { startDate: Date; endDate: Date; total: number[] | null; nullRatio: number[] }[];
	totalType?: 'integer' | 'decimal';
}): ITileSummarySeriesArgs {
	return generateSeriesData({
		key,
		startDate,
		endDate,
		points,
		normal,
		anomalies,
		totalType
	});
}

export function generateTileDriftSeriesData({
	key,
	startDate,
	endDate,
	points,
	normal,
	anomalies = [],
	totalType = 'integer'
}: {
	key: ITileSeriesKeyArgs;
	startDate: Date;
	endDate: Date;
	points: number;
	normal: {
		total: number[] | null;
		nullRatio: number[];
	};
	anomalies?: { startDate: Date; endDate: Date; total: number[] | null; nullRatio: number[] }[];
	totalType?: 'integer' | 'decimal';
}): ITileDriftSeriesArgs {
	return generateSeriesData({
		key,
		startDate,
		endDate,
		points,
		normal,
		anomalies,
		columns: {
			count: 'countChangePercentSeries',
			nullCount: 'nullRatioChangePercentSeries'
		},
		totalType
	});
}

function generateSeriesData({
	key,
	startDate,
	endDate,
	points,
	normal,
	anomalies = [],
	columns = {
		count: 'count',
		nullCount: 'nullCount'
	},
	totalType = 'integer'
}: {
	key: ITileSeriesKeyArgs;
	startDate: Date;
	endDate: Date;
	points: number;
	normal: {
		total: number[] | null;
		nullRatio: number[];
	};
	anomalies?: { startDate: Date; endDate: Date; total: number[] | null; nullRatio: number[] }[];
	columns?: {
		count: string;
		nullCount: string;
	};
	totalType?: 'integer' | 'decimal';
}) {
	const startDateTime = startDate.getTime();
	const endDateTime = endDate.getTime();
	const timeStep = (endDateTime - startDateTime) / (points - 1);

	const timestamps: number[] = [];
	const nullCount: number[] = [];
	const count: number[] = [];

	for (let i = 0; i < points; i++) {
		const timestamp = startDateTime + timeStep * i;
		timestamps.push(timestamp);

		const anomaly = anomalies.find(
			({ startDate, endDate }) => timestamp >= startDate.getTime() && timestamp <= endDate.getTime()
		);

		const randomNumber = totalType === 'decimal' ? randomUniform : randomInt;

		if (anomaly) {
			if (anomaly.total) {
				const total = randomNumber(anomaly.total[0], anomaly.total[1])();
				const nullRatio = randomUniform(anomaly.nullRatio[0], anomaly.nullRatio[1])();
				count.push(total * (1 - nullRatio));
				nullCount.push(total * nullRatio);
			}
		} else {
			if (normal.total) {
				const total = randomNumber(normal.total[0], normal.total[1])();
				const nullRatio = randomUniform(normal.nullRatio[0], normal.nullRatio[1])();
				count.push(total * (1 - nullRatio));
				nullCount.push(total * nullRatio);
			}
		}
	}

	return {
		key,
		timestamps,
		[columns.count]: count,
		[columns.nullCount]: nullCount
	};
}

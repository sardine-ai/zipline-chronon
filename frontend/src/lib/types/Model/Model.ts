export type Model = {
	name: string;
	online: boolean;
	production: boolean;
	team: string;
	modelType: string;
	join: Join;
};

export type ModelsResponse = {
	offset: number;
	items: Model[];
};

export type TimeSeriesItem = {
	value: number;
	nullValue: number;
	ts: number;
	label?: string | number;
};

export type TimeSeriesResponse = {
	id: string;
	items: TimeSeriesItem[];
};
export type Join = {
	name: string;
	joinFeatures: string[];
	groupBys: GroupBy[];
};

export type GroupBy = {
	name: string;
	features: string[];
};

export type JoinTimeSeriesResponse = {
	name: string; // todo: rename to joinName
	items: {
		name: string; // todo: rename to groupByName
		items: {
			feature: string;
			points: TimeSeriesItem[];
		}[];
	}[];
};

export type FeatureResponse = {
	feature: string;
	isNumeric?: boolean;
	points?: TimeSeriesItem[];
	baseline?: TimeSeriesItem[];
	current?: TimeSeriesItem[];
};

export type RawComparedFeatureResponse = {
	new: number[];
	old: number[];
	x: string[];
};

export type NullComparedFeatureResponse = {
	oldNullCount: number;
	newNullCount: number;
	oldValueCount: number;
	newValueCount: number;
};

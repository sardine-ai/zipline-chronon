export type Model = {
	name: string;
	online: boolean;
	production: boolean;
	team: string;
	modelType: string;
	join: JoinTimeSeriesResponse; // todo: this type needs to be updated to match the actual response once that WIP is finished
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
	points: TimeSeriesItem[];
};

export type RawComparedFeatureResponse = {
	feature: string;
	baseline: TimeSeriesItem[];
	current: TimeSeriesItem[];
};

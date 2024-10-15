export type Model = {
	name: string;
	id: string;
	online: boolean;
	production: boolean;
	team: string;
	modelType: string;
	createTime: number;
	lastUpdated: number;
};

export type ModelsResponse = {
	offset: number;
	items: Model[];
};

export type TimeSeriesItem = {
	value: number;
	ts: number;
	label: string | null;
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

import { Api } from '$lib/api/api';
import { ConfType, type IConfListResponseArgs } from '$lib/types/codegen';
import { entityConfig } from '$lib/types/Entity/Entity';

const ConfResponseMap: Record<ConfType, keyof IConfListResponseArgs> = {
	[ConfType.MODEL]: 'models',
	[ConfType.STAGING_QUERY]: 'stagingQueries',
	[ConfType.GROUP_BY]: 'groupBys',
	[ConfType.JOIN]: 'joins'
};

export async function load({ fetch, url, params }) {
	const path = url.pathname;
	const entityMatch = Object.values(entityConfig).find((c) =>
		params.conf.startsWith(c.path?.substring(1) ?? '')
	);

	if (!entityMatch || entityMatch.confType === null) {
		return {
			items: [],
			basePath: path,
			title: ''
		};
	}

	try {
		const api = new Api({ fetch });
		const response = await api.getConfList(entityMatch.confType);
		if (!response) throw new Error(`Failed to fetch ${entityMatch.label.toLowerCase()}`);

		const responseKey = ConfResponseMap[entityMatch.confType];
		const items = response[responseKey] ?? [];

		return {
			items: items,
			basePath: path,
			title: entityMatch.label
		};
	} catch (error) {
		console.error(`Failed to load ${entityMatch.label.toLowerCase()}:`, error);
		throw error;
	}
}

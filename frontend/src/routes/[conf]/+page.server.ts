import { error } from '@sveltejs/kit';

import { Api } from '$lib/api/api';
import { ConfType, type IConfListResponseArgs } from '$lib/types/codegen';
import { getEntityConfigFromPath } from '$src/lib/types/Entity';

const ConfResponseMap: Record<ConfType, keyof IConfListResponseArgs> = {
	[ConfType.MODEL]: 'models',
	[ConfType.STAGING_QUERY]: 'stagingQueries',
	[ConfType.GROUP_BY]: 'groupBys',
	[ConfType.JOIN]: 'joins'
};

export async function load({ fetch, url }) {
	const path = url.pathname;
	const entityConfig = getEntityConfigFromPath(path);

	if (!entityConfig || entityConfig.confType === null) {
		throw error(404, 'Not found');
	}

	try {
		const api = new Api({ fetch });
		const response = await api.getConfList(entityConfig.confType);
		if (!response) throw new Error(`Failed to fetch ${entityConfig.label.toLowerCase()}`);

		const responseKey = ConfResponseMap[entityConfig.confType];
		const items = response[responseKey] ?? [];

		return {
			items
		};
	} catch (error) {
		console.error(`Failed to load ${entityConfig.label.toLowerCase()}:`, error);
		throw error;
	}
}

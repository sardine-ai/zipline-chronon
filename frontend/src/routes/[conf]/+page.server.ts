import { Api } from '$lib/api/api';
import { ConfType } from '$lib/types/codegen';
import type { IConfListResponse } from '$lib/types/codegen/ConfListResponse';
import { entityConfig } from '$lib/types/Entity/Entity';

const ConfResponseMap: Record<ConfType, keyof IConfListResponse> = {
	[ConfType.MODEL]: 'models',
	[ConfType.STAGING_QUERY]: 'stagingQueries',
	[ConfType.GROUP_BY]: 'groupBys',
	[ConfType.JOIN]: 'joins'
};

export async function load({ fetch, url, params }) {
	const path = url.pathname;
	const entityMatch = entityConfig.find((entity) =>
		params.conf.startsWith(entity.path.substring(1))
	);

	if (!entityMatch) {
		return {
			items: [],
			basePath: path,
			title: ''
		};
	}

	try {
		const api = new Api({ fetch });
		const response = await api.getConfList(entityMatch.type);
		if (!response) throw new Error(`Failed to fetch ${entityMatch.label.toLowerCase()}`);

		const responseKey = ConfResponseMap[entityMatch.type];
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

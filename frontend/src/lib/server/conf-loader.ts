import { Api } from '$lib/api/api';
import { ConfType } from '$lib/types/codegen';
import type { IConfListResponse } from '$lib/types/codegen/ConfListResponse';
import type { RequestEvent } from '@sveltejs/kit';
import { entityConfig } from '$lib/types/Entity/Entity';

const ConfResponseMap: Record<ConfType, keyof IConfListResponse> = {
	[ConfType.MODEL]: 'models',
	[ConfType.STAGING_QUERY]: 'stagingQueries',
	[ConfType.GROUP_BY]: 'groupBys',
	[ConfType.JOIN]: 'joins'
};

export async function loadConfList({ fetch, url }: Pick<RequestEvent, 'fetch' | 'url'>) {
	const path = url.pathname;
	const entityMatch = entityConfig.find((entity) => path.startsWith(entity.path));

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

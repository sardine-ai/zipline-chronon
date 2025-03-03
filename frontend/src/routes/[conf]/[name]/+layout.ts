import type { PageServerLoad } from './$types';
import { Api } from '$lib/api/api';
import { ConfType } from '$src/lib/types/codegen';
import { getEntityConfigFromPath } from '$src/lib/types/Entity';
import { error } from '@sveltejs/kit';

function getConfApi(api: Api, confType: ConfType, confName: string) {
	switch (confType) {
		case ConfType.JOIN:
			return {
				conf: api.getJoin(confName),
				lineage: api.getJoinLineage({ name: confName })
			};
		case ConfType.GROUP_BY:
			return {
				conf: api.getGroupBy(confName),
				lineage: api.getGroupByLineage({ name: confName })
			};
		case ConfType.MODEL:
			return {
				conf: api.getModel(confName),
				lineage: api.getModelLineage({ name: confName })
			};
		case ConfType.STAGING_QUERY:
			return {
				conf: api.getStagingQuery(confName),
				lineage: api.getStagingQueryLineage({ name: confName })
			};
	}
}

export const load: PageServerLoad = async ({ params, fetch, url }) => {
	const api = new Api({ fetch });

	const confType = getEntityConfigFromPath(url.pathname)?.confType;

	if (confType) {
		const confApi = getConfApi(api, confType, params.name);
		const [conf, lineage] = await Promise.all([confApi?.conf, confApi?.lineage]);

		return {
			conf,
			lineage
		};
	} else {
		error(404, 'Not found');
	}
};

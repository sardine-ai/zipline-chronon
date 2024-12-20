import type { PageServerLoad } from './$types';
import type { JoinsResponse } from '$lib/types/Model/Model';
import { Api } from '$lib/api/api';

export const load: PageServerLoad = async ({ fetch }): Promise<{ joins: JoinsResponse }> => {
	const offset = 0;
	const limit = 100;
	const api = new Api({ fetch });
	return {
		joins: await api.getJoins(offset, limit)
	};
};

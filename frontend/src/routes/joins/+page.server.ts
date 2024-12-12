import type { PageServerLoad } from './$types';
import type { JoinsResponse } from '$lib/types/Model/Model';
import * as api from '$lib/api/api';

export const load: PageServerLoad = async (): Promise<{ joins: JoinsResponse }> => {
	const offset = 0;
	const limit = 100;
	return {
		joins: await api.getJoins(offset, limit)
	};
};

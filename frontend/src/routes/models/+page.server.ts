import type { PageServerLoad } from './$types';
import type { ModelsResponse } from '$lib/types/Model';
import * as api from '$lib/api/api';

export const load: PageServerLoad = async (): Promise<{ models: ModelsResponse }> => {
	return {
		models: await api.get('models')
	};
};

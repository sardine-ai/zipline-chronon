import type { PageServerLoad } from './$types';
import * as api from '$lib/api/api';
import type { TimeSeriesResponse } from '$lib/types/Model/Model';

export const load: PageServerLoad = async ({
	params
}): Promise<{ timeseries: TimeSeriesResponse }> => {
	return {
		timeseries: await api.getModelTimeseries(params.slug, 1725926400000, 1726106400000)
	};
};

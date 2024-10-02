import type { PageServerLoad } from './$types';
import * as api from '$lib/api/api';
import type { TimeSeriesResponse } from '$lib/types/Model';

export const load: PageServerLoad = async ({
	params
}): Promise<{ timeseries: TimeSeriesResponse }> => {
	// todo pass params dynamically
	return {
		timeseries: await api.get(
			`model/${params.slug}/timeseries?startTs=1725926400000&endTs=1726106400000&offset=10h&algorithm=psi`
		)
	};
};

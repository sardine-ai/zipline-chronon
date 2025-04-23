import { Api } from '$lib/api/api';
import { DEMO_DATE_START, DEMO_DATE_END } from '$src/lib/constants/common.js';

export async function load({ parent, fetch }) {
	const { lineage } = await parent();
	const api = new Api({ fetch });

	// Generate job tracker data for all nodes in lineage
	const lineageTasks = await api.getLineageJobTrackerData({
		name: lineage.mainNode?.name ?? 'Unknown',
		dateRange: {
			startDate: DEMO_DATE_START.toISOString(),
			endDate: DEMO_DATE_END.toISOString()
		} // TODO: read from query string
	});

	return { lineageTasks };
}

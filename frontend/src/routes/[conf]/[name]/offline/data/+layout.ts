import type { IJobTrackerResponseArgs } from '$lib/types/codegen';
import { Api } from '$lib/api/api';
import { DEMO_DATE_START, DEMO_DATE_END } from '$src/lib/constants/common';

export async function load({ parent, fetch }) {
	const { lineage } = await parent();
	const api = new Api({ fetch });

	// Get job tracker data for all nodes in lineage
	const nodes =
		Array.from(lineage.nodeGraph?.infoMap?.entries() ?? []).map(([key, info]) => ({
			key,
			info
		})) ?? [];

	// TODO: Lazy load on expansion.  How to handle summarizing upstream to
	const jobTrackerByNodeKeyName = new Map<string, IJobTrackerResponseArgs>();

	await Promise.all(
		Array.from(nodes).map(async (node) => {
			const data = await api.getJobTrackerData({
				name: node.key.name!,
				dateRange: {
					// TODO: read from query string
					startDate: DEMO_DATE_START.toISOString(),
					endDate: DEMO_DATE_END.toISOString()
				}
			});
			jobTrackerByNodeKeyName.set(node.key.name!, data);
		})
	);

	return {
		jobTrackerByNodeKeyName
	};
}

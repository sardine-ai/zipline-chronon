import type { PageLoad } from './$types';
import { buildJobTrackerTree } from '$lib/job/tree-builder/tree-builder';

export const load: PageLoad = async ({ parent }) => {
	const { lineage, jobTrackerDataMap } = await parent();

	const jobTrackerData = buildJobTrackerTree(lineage, jobTrackerDataMap);

	return {
		jobTree: jobTrackerData.jobTree,
		dates: jobTrackerData.dates
	};
};

import type { PageLoad } from './$types';
import { buildJobTrackerTree } from '$lib/job/tree-builder/tree-builder';
import type { IJobTrackerResponseArgs, INodeKeyArgs } from '$lib/types/codegen';
import { Api } from '$lib/api/api';

export const load: PageLoad = async ({ parent, fetch }) => {
	const { lineage } = await parent();
	const api = new Api({ fetch });

	// Get job tracker data for all nodes in lineage
	const nodes = new Set<INodeKeyArgs>();

	// Add main nodes
	lineage.nodeGraph?.connections?.forEach((_, node) => {
		nodes.add(node);
	});

	// Add parent nodes
	lineage.nodeGraph?.connections?.forEach((connection) => {
		connection.parents?.forEach((parent) => {
			nodes.add(parent);
		});
	});

	// todo think about where this is best called - might be worth lazily lower levels
	const jobTrackerDataMap = new Map<string, IJobTrackerResponseArgs>();
	await Promise.all(
		Array.from(nodes).map(async (node) => {
			const data = await api.getJobTrackerData(node);
			jobTrackerDataMap.set(node.name ?? '', data);
		})
	);

	const jobTrackerData = buildJobTrackerTree(lineage, jobTrackerDataMap);

	return {
		jobTree: jobTrackerData.jobTree,
		dates: jobTrackerData.dates
	};
};

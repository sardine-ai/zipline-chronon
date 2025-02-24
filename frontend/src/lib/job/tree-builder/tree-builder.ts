import type {
	IJobTrackerResponseArgs,
	ILineageResponseArgs,
	ITaskInfoArgs,
	INodeKeyArgs,
	INodeInfoArgs
} from '$lib/types/codegen';
import type { NodeConfiguration } from '$src/lib/types/LogicalNode';

/**
 * Represents a node in the job tracker's hierarchical tree structure.
 * Used to display jobs and their dependencies in a collapsible tree view.
 */
export interface JobTreeNode {
	row: string;
	node?: INodeKeyArgs;
	conf?: NodeConfiguration;
	jobTracker?: IJobTrackerResponseArgs & {
		tasksByDate?: ITaskInfoArgs[];
	};
	children: JobTreeNode[];
}

/**
 * Finds a node by name in the lineage connections
 */
const findNodeInConnections = (name: string, lineage: ILineageResponseArgs) => {
	if (!lineage.nodeGraph?.connections) return null;
	for (const [node] of lineage.nodeGraph.connections) {
		if (node.name === name) {
			return node;
		}
	}
	return null;
};

/**
 * Collects all dates between min and max dates from job tracker responses
 */
const getAllDates = (responses: IJobTrackerResponseArgs[]): string[] => {
	const dates = new Set<string>();
	responses.forEach((response) => {
		response.tasks?.forEach((task) => {
			if (task.dateRange?.startDate && task.dateRange?.endDate) {
				dates.add(task.dateRange.startDate);
				dates.add(task.dateRange.endDate);
			}
		});
	});

	if (dates.size === 0) return [];

	// Convert to array and find min/max dates
	const dateArray = Array.from(dates).sort();
	const startDate = new Date(dateArray[0]);
	const endDate = new Date(dateArray[dateArray.length - 1]);

	// Generate all dates between start and end
	const allDates: string[] = [];
	const currentDate = new Date(startDate);

	while (currentDate <= endDate) {
		allDates.push(currentDate.toISOString().split('T')[0]);
		currentDate.setDate(currentDate.getDate() + 1);
	}

	return allDates;
};

/**
 * Finds leaf nodes in the lineage (nodes that don't appear as parents)
 */
const findLeafNodes = (lineage: ILineageResponseArgs): string[] => {
	const allNodes = new Set(
		Array.from(lineage.nodeGraph?.connections?.keys() ?? [])
			.map((node) => node.name)
			.filter((name): name is string => name !== undefined)
	);
	const allParents = new Set<string>();

	lineage.nodeGraph?.connections?.forEach((connection) => {
		connection.parents?.forEach((parent) => {
			allParents.add(parent.name ?? '');
		});
	});

	return Array.from(allNodes).filter((node) => !allParents.has(node));
};

/**
 * Organizes tasks by date ranges, keeping only the latest task for overlapping periods
 */
export const organizeTasksByDate = (tasks: ITaskInfoArgs[] = []): ITaskInfoArgs[] => {
	if (tasks.length === 0) return [];

	// Sort tasks by submittedTs in descending order (most recent first)
	const sortedTasks = [...tasks].sort((a, b) => Number(b.submittedTs) - Number(a.submittedTs));

	const result: ITaskInfoArgs[] = [];

	for (const task of sortedTasks) {
		if (!task.dateRange?.startDate || !task.dateRange?.endDate) continue;

		let currentStart = new Date(task.dateRange.startDate);
		let currentEnd = new Date(task.dateRange.endDate);

		// Check for overlaps with more recent tasks
		for (const existingTask of result) {
			const existingStart = new Date(existingTask.dateRange!.startDate!);
			const existingEnd = new Date(existingTask.dateRange!.endDate!);

			// If current range starts after existing range ends, no overlap
			if (currentStart > existingEnd) continue;

			// If current range ends before existing range starts, no overlap
			if (currentEnd < existingStart) continue;

			// Handle overlap by adjusting current range
			if (currentStart < existingStart) {
				// Current task starts earlier - keep only the non-overlapping part
				currentEnd = new Date(existingStart);
				currentEnd.setDate(currentEnd.getDate() - 1);
			} else {
				// Current task starts within or at same time - skip this range
				currentStart = new Date(existingEnd);
				currentStart.setDate(currentStart.getDate() + 1);
			}

			// If the range has been eliminated, break
			if (currentStart > currentEnd) break;
		}

		// Add the remaining range if it's valid
		if (currentStart <= currentEnd) {
			result.push({
				...task,
				dateRange: {
					startDate: currentStart.toISOString().split('T')[0],
					endDate: currentEnd.toISOString().split('T')[0]
				}
			});
		}
	}

	// Sort by start date for consistent ordering
	return result.sort(
		(a, b) =>
			new Date(a.dateRange!.startDate!).getTime() - new Date(b.dateRange!.startDate!).getTime()
	);
};

/**
 * Recursively builds a tree node with its children
 */
const buildJobTreeNode = (
	jobName: string,
	lineage: ILineageResponseArgs,
	jobTrackerDataMap: Map<string, IJobTrackerResponseArgs>,
	infoMap: Map<INodeKeyArgs, INodeInfoArgs>
): JobTreeNode => {
	const node = findNodeInConnections(jobName, lineage);
	const jobTrackerData = jobTrackerDataMap.get(jobName);
	const nodeInfo = Array.from(infoMap.entries()).find(([key]) => key.name === jobName);

	// Organize tasks by date before adding to the tree
	const tasksByDate = organizeTasksByDate(jobTrackerData?.tasks);

	if (!node) {
		return {
			row: jobName,
			conf: nodeInfo?.[1].conf,
			jobTracker: jobTrackerData
				? {
						...jobTrackerData,
						tasksByDate
					}
				: undefined,
			children: [],
			node: undefined
		};
	}

	const parentNodes =
		Array.from(lineage.nodeGraph?.connections?.entries() ?? []).find(
			([n]) => n.name === jobName
		)?.[1]?.parents ?? [];

	return {
		row: jobName,
		conf: nodeInfo?.[1].conf,
		jobTracker: jobTrackerData
			? {
					...jobTrackerData,
					tasksByDate
				}
			: undefined,
		children: parentNodes.map((parent) =>
			buildJobTreeNode(parent.name ?? '', lineage, jobTrackerDataMap, infoMap)
		),
		node: { ...node }
	};
};

/**
 * Transforms lineage and job tracker data into a tree structure for display
 */
export function buildJobTrackerTree(
	lineage: ILineageResponseArgs,
	jobTrackerDataMap: Map<string, IJobTrackerResponseArgs>
) {
	if (!lineage) {
		throw new Error('Lineage data is required for job tracking');
	}

	if (!lineage.nodeGraph?.infoMap) {
		throw new Error('Node info map is required for job tracking');
	}

	const leafNodes = findLeafNodes(lineage);
	const jobTree = leafNodes.map((leafNode) =>
		buildJobTreeNode(leafNode, lineage, jobTrackerDataMap, lineage.nodeGraph!.infoMap!)
	);
	const dates = getAllDates(Array.from(jobTrackerDataMap.values()));

	return { jobTree, dates };
}

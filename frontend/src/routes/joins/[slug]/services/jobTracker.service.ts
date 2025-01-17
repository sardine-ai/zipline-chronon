import {
	generateJobRunListSampleData,
	generateJobDependencyGraphSampleData
} from '$lib/util/job-sample-data';
import type { JobTreeNode, JobRun, JobRunList } from '$lib/types/Job/Job';

const addRunsToTree =
	(jobRunList: JobRunList, adjacencyList: Record<string, string[]>) =>
	(row: string): JobTreeNode => {
		return {
			row,
			runs: jobRunList[row] || [],
			children: (adjacencyList[row] || []).map((childRow) =>
				addRunsToTree(jobRunList, adjacencyList)(childRow)
			)
		};
	};

const getAllDates = (runs: JobRun[]): string[] => {
	const dates = new Set<string>();
	runs.forEach((run) => {
		dates.add(run.start);
		dates.add(run.end);
	});
	return Array.from(dates).sort();
};

export async function getJobTrackerData() {
	const jobRunListSampleData = generateJobRunListSampleData(10, 21); // todo: get from api
	const { adjacencyList, rootNodes } = generateJobDependencyGraphSampleData(jobRunListSampleData); // todo: get from api

	// preprocess data into tree structure
	const jobTree = rootNodes.map(addRunsToTree(jobRunListSampleData, adjacencyList));
	const dates = getAllDates(Object.values(jobRunListSampleData).flat());

	return { jobTree, dates };
}

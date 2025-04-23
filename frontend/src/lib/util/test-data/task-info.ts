import { randomInteger } from '@layerstack/utils';
import {
	Status,
	type ITaskInfoArgs,
	type ILineageResponseArgs,
	type INodeKeyArgs
} from '../../types/codegen';
import { timeDay } from 'd3';
import { startOfDay, endOfDay } from 'date-fns';

/**
 * Consider upstream (parents) status for each node's status
 */
export function generateLineageTaskInfoData({
	node,
	lineage,
	startDate,
	endDate
}: {
	node: INodeKeyArgs;
	lineage: ILineageResponseArgs | null;
	startDate: Date;
	endDate: Date;
}): Record<string, ITaskInfoArgs[]> {
	let tasks: Record<string, ITaskInfoArgs[]> = {};

	// const node = lineage?.mainNode;
	const parents = node ? (lineage?.nodeGraph?.connections?.get(node)?.parents ?? []) : [];

	// Resurively generate tasks for each parent
	let upstreamTasks: Record<string, ITaskInfoArgs[]> = {};
	for (const parent of parents ?? []) {
		const parentTasks = generateLineageTaskInfoData({
			node: parent,
			lineage,
			startDate,
			endDate
		});

		upstreamTasks = { ...upstreamTasks, ...parentTasks };
	}
	tasks = { ...upstreamTasks, ...tasks };

	tasks[node.name ?? 'Unknown'] = generateTaskInfoData({
		startDate,
		endDate,
		upstreamTasks
	});

	return tasks;
}

export function generateTaskInfoData({
	startDate,
	endDate,
	upstreamTasks
}: {
	startDate: Date;
	endDate: Date;
	upstreamTasks: Record<string, ITaskInfoArgs[]>;
}): ITaskInfoArgs[] {
	const tasks: ITaskInfoArgs[] = [];

	const upstreamStatuses = Object.values(upstreamTasks).map((tasks) => {
		return tasks.map((t) => t.status);
	});

	const days = timeDay.range(startDate, endDate);

	for (const [i, day] of days.entries()) {
		// Respect upstream when generating task status
		const upstreamFailure = upstreamStatuses.some((upstream) => {
			return upstream[i] === Status.FAILED;
		});

		const upstreamRunning = upstreamStatuses.some((upstream) => {
			return upstream[i] === Status.RUNNING;
		});

		const upstreamQueued = upstreamStatuses.some((upstream) => {
			return upstream[i] === Status.QUEUED;
		});

		const previousStatus = tasks[i - 1]?.status;

		const status = upstreamFailure
			? Status.FAILED
			: upstreamRunning
				? Status.RUNNING
				: upstreamQueued || previousStatus === Status.QUEUED
					? Status.QUEUED
					: i < days.length * 0.9
						? Status.SUCCESS
						: getRandomStatus();

		// Submitted sometime in the last 8-24 hours
		const submittedTimeOffsetMin = randomInteger(8 * 60 * 60, 24 * 60 * 60);

		// Started sometime in the last 1-24 hours
		const startedTimeOffsetMin = Math.min(
			submittedTimeOffsetMin,
			randomInteger(1 * 60 * 60, 24 * 60 * 60)
		);

		// Started sometime in the last 1-24 hours
		const finishedTimeOffsetMin = Math.min(
			startedTimeOffsetMin,
			randomInteger(1 * 60 * 60, 1 * 60 * 60)
		);

		const task: ITaskInfoArgs = {
			dateRange: {
				startDate: startOfDay(day).toISOString(),
				endDate: endOfDay(day).toISOString()
			},
			status,

			// TODO: Improve generated properties
			logPath: '/var/log/...',
			trackerUrl: 'https://example.com/...',
			taskArgs: {
				argsList: [],
				env: new Map()
			},

			submittedTs: new Date().getTime() - submittedTimeOffsetMin * 1000,
			startedTs: ![Status.QUEUED].includes(status)
				? day.getTime() - startedTimeOffsetMin * 1000
				: undefined,
			finishedTs: [Status.SUCCESS, Status.FAILED].includes(status)
				? day.getTime() - finishedTimeOffsetMin * 1000
				: undefined,

			user: 'user',
			team: 'team',

			allocatedResources: {
				cumulativeDiskReadBytes: 1,
				cumulativeDiskWriteBytes: 1,
				megaByteSeconds: 1,
				vcoreSeconds: 1
			},
			utilizedResources: {
				cumulativeDiskReadBytes: 1,
				cumulativeDiskWriteBytes: 1,
				megaByteSeconds: 1,
				vcoreSeconds: 1
			}
		};
		tasks.push(task);
	}

	return tasks;
}

// function getRandomStatus(): Status {
// 	const rand = Math.random();
// 	if (rand < 0.02) {
// 		// 2% RUNNING
// 		return Status.RUNNING;
// 	} else if (rand < 0.07) {
// 		// 5% QUEUED
// 		return Status.QUEUED;
// 		} else if (rand < 0.075) {
// 			// 0.5% FAILED
// 			return Status.FAILED;
// 	}
// 	return Status.SUCCESS; // 92.5% SUCCESS
// }

function getRandomStatus(): Status {
	const rand = Math.random();
	if (rand < 0.1) {
		// 10% SUCCESS
		return Status.SUCCESS;
	}
	if (rand < 0.5) {
		// 50% RUNNING
		return Status.RUNNING;
	}
	return Status.QUEUED; // 40% QUEUED
}

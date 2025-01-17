export type JobStatus = 'waiting' | 'running' | 'failed' | 'completed' | 'queued' | 'invalid';

// this will come from thrift/api
export type JobRunList = Record<string, JobRun[]>; // maps job_id to list of its runs

export type JobRun = {
	start: string; // ISO date string
	end: string; // ISO date string
	status: JobStatus;
};

// this will come from thrift/api
export type JobDependencyGraph = {
	adjacencyList: Record<string, string[]>; // maps job_id to list of children job_ids
	rootNodes: string[]; // list of job_ids that are root nodes
};

// this is how the frontend will transform the data for the job tracker table
export type JobTreeNode = {
	row: string;
	children: JobTreeNode[];
	runs: JobRun[];
};

// todo: update these colors to match the design
export const statusColors: Record<JobStatus, string> = {
	waiting: 'bg-jobs-waiting-bg',
	running: 'bg-jobs-running-bg',
	failed: 'bg-jobs-failed-bg',
	completed: 'bg-jobs-completed-bg',
	queued: 'bg-jobs-queued-bg',
	invalid: 'bg-jobs-invalid-bg'
};

export const statusBorderColors: Record<JobStatus, string> = {
	waiting: 'border border-jobs-waiting-text',
	running: 'border border-jobs-running-text',
	failed: 'border border-jobs-failed-text',
	completed: 'border border-jobs-completed-text',
	queued: 'border border-jobs-queued-text',
	invalid: 'border border-jobs-invalid-text'
};

export const statusRingColors: Record<JobStatus, string> = {
	waiting: 'ring-jobs-waiting-text',
	running: 'ring-jobs-running-text',
	failed: 'ring-jobs-failed-text',
	completed: 'ring-jobs-completed-text',
	queued: 'ring-jobs-queued-text',
	invalid: 'ring-jobs-invalid-text'
};

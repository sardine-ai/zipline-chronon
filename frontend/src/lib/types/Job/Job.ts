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

export const statusColors: Record<JobStatus, string> = {
	waiting: 'bg-jobs-waiting-bg',
	running: 'bg-jobs-running-bg',
	failed: 'bg-jobs-failed-bg',
	completed: 'bg-jobs-completed-bg',
	queued: 'bg-jobs-queued-bg',
	invalid: 'bg-jobs-invalid-bg'
};

export const statusBorderColors: Record<JobStatus, string> = {
	waiting: 'border-jobs-waiting-border',
	running: 'border-jobs-running-border',
	failed: 'border-jobs-failed-border',
	completed: 'border-jobs-completed-border',
	queued: 'border-jobs-queued-border',
	invalid: 'border-jobs-invalid-border'
};

export const statusActiveBorderColors: Record<JobStatus, string> = {
	waiting: 'border-jobs-waiting-active-border',
	running: 'border-jobs-running-active-border',
	failed: 'border-jobs-failed-active-border',
	completed: 'border-jobs-completed-active-border',
	queued: 'border-jobs-queued-active-border',
	invalid: 'border-jobs-invalid-active-border'
};

import { Status } from '$lib/types/codegen';

export const statusText: Record<Status, string> = {
	[Status.WAITING_FOR_UPSTREAM]: 'Waiting for Upstream',
	[Status.RUNNING]: 'Running',
	[Status.FAILED]: 'Failed',
	[Status.SUCCESS]: 'Success',
	[Status.QUEUED]: 'Queued',
	[Status.UPSTREAM_FAILED]: 'Upstream Failed',
	[Status.UPSTREAM_MISSING]: 'Upstream Missing',
	[Status.WAITING_FOR_RESOURCES]: 'Waiting for Resources'
};

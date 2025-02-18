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

export function getStatusColorVariable(
	status: Status,
	type: 'bg' | 'border' | 'active-border'
): string {
	const statusString = Status[status];
	return `--job-${statusString}-${type}`;
}

export function getStripeBackground(status: Status | undefined): string {
	if (!status) return '';

	const needsStripes = status === Status.UPSTREAM_FAILED || status === Status.WAITING_FOR_RESOURCES;
	if (!needsStripes) return '';

	const baseColor = getStatusColorVariable(status, 'bg');
	const stripePattern = `repeating-linear-gradient(
		135deg,
		hsl(var(${baseColor})) 0 10px,
		transparent 0 20px
	)`;
	const baseLayer = `hsl(var(${baseColor}) / 0.3)`;

	return `background: ${stripePattern}, ${baseLayer}`;
}

export function getStatusClasses(
	status: Status | undefined,
	isActive: boolean,
	includeHover: boolean
): string {
	if (status === undefined) return '';

	const statusString = Status[status];

	const classes = [
		`bg-jobs-${statusString}-bg`,
		isActive ? `border-jobs-${statusString}-active-border` : `border-jobs-${statusString}-border`
	];

	if (includeHover) {
		classes.push(`hover:border-jobs-${statusString}-active-border`);
	}

	return classes.join(' ');
}

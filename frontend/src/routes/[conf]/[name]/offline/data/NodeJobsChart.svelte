<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { BrushContext, Chart, Html, Tooltip } from 'layerchart';
	import type { DomainType } from 'layerchart/utils/scales.svelte';
	import { cls } from '@layerstack/tailwind';
	import { parseISO } from 'date-fns';

	import { type IJobTrackerResponseArgs } from '$src/lib/types/codegen';
	import JobStatusRect from './JobStatusRect.svelte';
	import { format, PeriodType } from '@layerstack/utils';
	import { Button, Drawer } from 'svelte-ux';
	import PropertyList from '$src/lib/components/PropertyList.svelte';
	import { tooltipProps } from '$src/lib/components/charts/common';

	import IconXMark from '~icons/heroicons/x-mark';
	import IconMousePointerClick from '~icons/lucide/mouse-pointer-click';

	let {
		jobTracker,
		xDomain,
		onBrushEnd
	}: {
		jobTracker: IJobTrackerResponseArgs;
		xDomain?: DomainType;
		onBrushEnd?: ComponentProps<typeof BrushContext>['onBrushEnd'];
	} = $props();

	let chartData = $derived(
		jobTracker.tasks?.map((t) => ({
			startDate: parseISO(t.dateRange?.startDate ?? ''),
			endDate: parseISO(t.dateRange?.endDate ?? ''),
			status: t.status,
			data: t
		})) ?? []
	);

	let selectedTask = $state<(typeof chartData)[number] | null>(null);
</script>

<Chart
	data={chartData}
	x={['startDate', 'endDate']}
	{xDomain}
	brush={{
		resetOnEnd: true,
		ignoreResetClick: true, // allow clicking on task without resetting brush
		onBrushEnd: onBrushEnd
	}}
>
	{#snippet children({ context })}
		<Html class="overflow-hidden">
			{#each chartData as d}
				{@const x = context.xScale(d.startDate)}
				{@const width = context.xScale(d.endDate) - x}
				{@const height = context.height}

				<JobStatusRect
					status={d.status}
					class="absolute"
					style="top:0; left:{x}px; width:{width}px; height:{height}px;"
					onpointerenter={(e) => {
						context.tooltip.show(e, d);
					}}
					onpointerleave={() => {
						context.tooltip.hide();
					}}
					onclick={(e) => {
						selectedTask = d;
					}}
				/>
			{/each}
		</Html>

		<Tooltip.Root {...tooltipProps.root} y={context.height + 24} pointerEvents>
			{#snippet children({ data })}
				<Tooltip.Header class={cls(tooltipProps.header.class, 'grid grid-cols-[1fr_auto] gap-2')}>
					<div>
						{data ? format(context.x(data)[0], PeriodType.Day) : ''}
					</div>
					<JobStatusRect status={data?.status} includeLabel />
				</Tooltip.Header>

				<Tooltip.List class={cls(tooltipProps.list, 'px-3 my-2 gap-y-2 whitespace-nowrap')}>
					{#if data?.data.submittedTs}
						<Tooltip.Item
							{...tooltipProps.item}
							label="Submitted"
							value={new Date(data.data.submittedTs as number)}
							format={PeriodType.DayTime}
						/>
					{/if}

					{#if data?.data.startedTs}
						<Tooltip.Item
							{...tooltipProps.item}
							label="Started"
							value={new Date(data.data.startedTs as number)}
							format={PeriodType.DayTime}
						/>
					{/if}

					{#if data?.data.finishedTs}
						<Tooltip.Item
							{...tooltipProps.item}
							label="Finished"
							value={new Date(data.data.finishedTs as number)}
							format={PeriodType.DayTime}
						/>
					{/if}
				</Tooltip.List>

				<div class="flex gap-2 text-foreground text-xs px-2 py-2 border-t">
					<IconMousePointerClick />
					Click to view details
				</div>
			{/snippet}
		</Tooltip.Root>
	{/snippet}
</Chart>

<Drawer
	open={selectedTask != null}
	on:close={() => {
		selectedTask = null;
	}}
	let:close
	classes={{
		root: 'border-l'
	}}
>
	{#if selectedTask}
		<div class="w-[1000px] max-w-[90vw] px-6">
			<header class="sticky top-0 z-10 bg-surface-100 pt-4 mb-3">
				<div class="grid grid-cols-[1fr_auto] items-center gap-3">
					<div>
						<div class="text-lg">
							{jobTracker.mainNode?.name}
							<JobStatusRect status={selectedTask.status} includeLabel class="ml-2" />
						</div>
						<div class="text-sm">{format(selectedTask.startDate, PeriodType.Day)}</div>
					</div>

					<Button class="absolute top-2 -right-4 w-8 h-8 p-0" on:click={() => close()}>
						<IconXMark />
						<span class="sr-only">Close</span>
					</Button>
				</div>
			</header>

			<div class="grid gap-4 text-sm overflow-auto">
				<div>
					<div class="font-semibold py-2">Timeline</div>
					<PropertyList
						items={[
							{
								label: 'Submited',
								value: format(new Date(selectedTask.data.submittedTs as number), PeriodType.DayTime)
							},
							selectedTask.data.startedTs
								? {
										label: 'Started',
										value: format(
											new Date(selectedTask.data.startedTs as number),
											PeriodType.DayTime
										)
									}
								: null,
							selectedTask.data.finishedTs
								? {
										label: 'Finished',
										value: format(
											new Date(selectedTask.data.finishedTs as number),
											PeriodType.DayTime
										)
									}
								: null
						]}
						align="right"
					/>
				</div>

				<div>
					<div class="font-semibold py-2">Details</div>
					<PropertyList
						items={[
							{
								label: 'Arguments',
								value: selectedTask.data.taskArgs?.argsList?.join(', ')
							},
							{
								label: 'Env',
								value: Array.from(selectedTask.data.taskArgs?.env ?? [])
									.map(([k, v]) => `${k}=${v}`)
									.join(', ')
							},
							{
								label: 'Logs',
								value: selectedTask.data.logPath
							},
							{
								label: 'Tracker URL',
								value: selectedTask.data.trackerUrl
							},
							{
								label: 'User',
								value: selectedTask.data.user
							},
							{
								label: 'Team',
								value: selectedTask.data.team
							}
						]}
						align="right"
					/>
				</div>

				{#if selectedTask.data.allocatedResources}
					<div>
						<div class="font-semibold py-2">Allocated Resources</div>
						<PropertyList
							items={[
								{
									label: 'Cumulative disk read bytes',
									value: selectedTask.data.allocatedResources.cumulativeDiskReadBytes
								},
								{
									label: 'Cumulative disk write bytes',
									value: selectedTask.data.allocatedResources.cumulativeDiskWriteBytes
								},
								{
									label: 'Mega byte seconds',
									value: selectedTask.data.allocatedResources.megaByteSeconds
								},
								{
									label: 'Vcore seconds',
									value: selectedTask.data.allocatedResources.vcoreSeconds
								}
							]}
							align="right"
						/>
					</div>
				{/if}

				{#if selectedTask.data.utilizedResources}
					<div>
						<div class="font-semibold py-2">Utilized Resources</div>
						<PropertyList
							items={[
								{
									label: 'Cumulative disk read bytes',
									value: selectedTask.data.utilizedResources.cumulativeDiskReadBytes
								},
								{
									label: 'Cumulative disk write bytes',
									value: selectedTask.data.utilizedResources.cumulativeDiskWriteBytes
								},
								{
									label: 'Mega byte seconds',
									value: selectedTask.data.utilizedResources.megaByteSeconds
								},
								{
									label: 'Vcore seconds',
									value: selectedTask.data.utilizedResources.vcoreSeconds
								}
							]}
							align="right"
						/>
					</div>
				{/if}

				<div>
					<div class="font-semibold py-2">Cost</div>
					<PropertyList
						items={[
							{
								label: 'TODO',
								value: ''
							}
						]}
						align="right"
					/>
				</div>
			</div>
		</div>
	{/if}
</Drawer>

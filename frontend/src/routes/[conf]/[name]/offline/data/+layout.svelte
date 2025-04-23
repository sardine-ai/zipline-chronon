<script lang="ts">
	import { Button, Popover, Toggle } from 'svelte-ux';
	import { Axis, Chart, Html, Svg } from 'layerchart';
	import { scaleTime } from 'd3';
	import { format, PeriodType } from '@layerstack/utils';
	import { parseISO } from 'date-fns';

	import { page } from '$app/state';

	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import { Separator } from '$lib/components/ui/separator';
	import PropertyList from '$lib/components/PropertyList.svelte';

	import JobsStatusLegend from './JobsStatusLegend.svelte';
	import NodeJobsRow from './NodeJobsRow.svelte';

	import JobStatusRect from './JobStatusRect.svelte';
	import { Status } from '$src/lib/types/codegen';

	import IconArrowDownOnSquare16Solid from '~icons/heroicons/arrow-down-on-square-16-solid';
	import IconInformationCircle from '~icons/heroicons/information-circle';

	import { shared } from '../../shared.svelte';

	let { data, children } = $props();

	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	let [_, conf, name, tab, primaryNav, secondaryNav] = $derived(page.url.pathname.split('/'));

	const jobTracker = $derived(data.jobTrackerByNodeKeyName.get(data.lineage.mainNode?.name ?? ''));

	const tasksChartData = $derived(
		jobTracker?.tasks?.map((t) => ({
			startDate: parseISO(t.dateRange?.startDate ?? ''),
			endDate: parseISO(t.dateRange?.endDate ?? ''),
			status: t.status,
			data: t
		})) ?? []
	);
</script>

<div class="flex flex-col">
	<CollapsibleSection open class="mt-5 mb-6">
		{#snippet trigger()}
			<div class="flex-1">Current job</div>
			<Button
				size="sm"
				class="text-muted-foreground gap-2 border rounded-md"
				on:click={(e) => {
					e.stopPropagation();
					alert('TODO: download logs');
				}}
			>
				<IconArrowDownOnSquare16Solid class="size-4 mb-1" />
				Download Logs
			</Button>
		{/snippet}

		<!-- TODO: use real data from API -->
		<div class="grid gap-4 grid-cols-2">
			<PropertyList
				items={[
					{ label: 'Job ID', value: 'chargeback_v1_adhoc_2024-01-03__2024-01-07' },
					{ label: 'Status', value: Status.RUNNING, type: 'status' },
					{ label: 'Start date & time', value: 'Oct 08, 2024 4:10pm' }
				]}
				align="right"
			/>
			<PropertyList
				items={[
					{ label: 'Run by', value: 'Nikhil Simha' },
					{ label: 'Adhoc / Scheduled', value: 'Adhoc' },
					{ label: 'Type', value: 'Batch upload' }
				]}
				align="right"
			/>
		</div>
	</CollapsibleSection>

	<Separator fullWidthExtend={true} wide={true} />

	<CollapsibleSection open class="my-4">
		{#snippet trigger()}
			<div class="flex-1">Timeline</div>

			<Toggle let:on={open} let:toggle let:toggleOff>
				<Button
					class="text-muted-foreground"
					on:click={(e) => {
						e.stopPropagation();
						toggle();
					}}
				>
					<IconInformationCircle class="h-4 w-4" />
					<span class="text-xs">Status Legend</span>
				</Button>

				<Popover
					{open}
					on:close={toggleOff}
					placement="top-end"
					class="bg-neutral-100 border shadow-md rounded-md p-4"
					offset={4}
				>
					<JobsStatusLegend />
				</Popover>
			</Toggle>
		{/snippet}

		<!-- Overview with shared brush for all charts -->
		<div class="h-[36px] border-y">
			<Chart
				data={tasksChartData}
				x={['startDate', 'endDate']}
				xScale={scaleTime()}
				brush={{
					mode: 'separated',
					xDomain: shared.xDomain,
					onChange: (e) => {
						shared.xDomain = e.xDomain;
					}
				}}
			>
				{#snippet children({ context })}
					<Html>
						{#each tasksChartData as d}
							{@const x = context.xScale(d.startDate)}
							{@const width = context.xScale(d.endDate) - x}
							{@const height = context.height}

							<div
								class="absolute py-2"
								style="top: 0; left: {x}px; width: {width}px; height: {height}px;"
							>
								<JobStatusRect status={d.status} class="size-full" />
							</div>
						{/each}
					</Html>
				{/snippet}
			</Chart>
		</div>

		<div class="grid grid-cols-[320px_1fr] gap-2">
			<div></div>

			<!-- Shared x-axis for all charts -->
			<div class="h-[24px]">
				<Chart
					data={tasksChartData}
					x={['startDate', 'endDate']}
					xScale={scaleTime()}
					padding={{ top: 20 }}
					xDomain={shared.xDomain}
				>
					<Svg>
						<Axis placement="top" format={(v) => format(v, PeriodType.Day, { variant: 'short' })} />
					</Svg>
				</Chart>
			</div>
		</div>

		<NodeJobsRow
			node={data.lineage.mainNode!}
			jobTrackerByNodeKeyName={data.jobTrackerByNodeKeyName}
			lineage={data.lineage}
			xDomain={shared.xDomain}
			onBrushEnd={(e) => {
				shared.xDomain = e.xDomain;
			}}
		/>
	</CollapsibleSection>

	<Separator fullWidthExtend wide />

	<CollapsibleSection label="Data Quality" open class="mt-4">
		{@render children()}
	</CollapsibleSection>
</div>

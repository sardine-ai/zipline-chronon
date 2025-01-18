<script lang="ts">
	import { readable } from 'svelte/store';
	import { createTable, Render, createRender, DataBodyRow } from 'svelte-headless-table';
	import { addSubRows, addExpandedRows } from 'svelte-headless-table/plugins';
	import ExpandableCell from '$lib/components/ExpandableCell.svelte';
	import StatusCell from '$lib/components/StatusCell.svelte';
	import CollapsibleSection from '$lib/components/CollapsibleSection.svelte';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import { type SvelteComponent } from 'svelte';
	import { type JobTreeNode } from '$lib/types/Job/Job';
	import { onMount } from 'svelte';
	import { Separator } from '$lib/components/ui/separator';
	import LogsDownloadButton from '$lib/components/LogsDownloadButton.svelte';
	import JobOverviewChart from '$lib/components/JobOverviewChart.svelte';

	let { data } = $props();

	const jobsData = readable<JobTreeNode[]>(data.jobTree);

	const table = createTable(jobsData, {
		sub: addSubRows({
			children: 'children'
		}),
		expand: addExpandedRows()
	});

	const columns = table.createColumns([
		table.display({
			id: 'job-structure',
			header: 'Job structure',
			cell: ({ row }, { pluginStates }) => {
				const { isExpanded, canExpand, isAllSubRowsExpanded } =
					pluginStates.expand.getRowState(row);
				const jobId = (row as DataBodyRow<JobTreeNode>).original.row;
				const childrenCount = (row as DataBodyRow<JobTreeNode>).original.children?.length || 0;

				return createRender(ExpandableCell as unknown as typeof SvelteComponent, {
					depth: row.depth,
					isExpanded,
					canExpand,
					isAllSubRowsExpanded,
					name: jobId,
					childrenCount
				});
			}
		})
	]);

	const { rows } = table.createViewModel(columns);

	function getDaysInRange(start: string, end: string, availableDates: string[]): number {
		return availableDates.filter((date) => date >= start && date <= end).length;
	}

	function formatDate(dateString: string): string {
		const date = new Date(dateString);
		const month = date.toLocaleDateString('en-US', { month: 'short' });
		const day = date.getDate();
		const year = date.getFullYear();
		return `${month} ${day}\n${year}`;
	}

	const stickyCellClass = 'sticky left-0 bg-neutral-100 z-30';
	const tableHeadClass = 'h-[42px] px-2';
	const tableCellClass = 'p-0 border-0 h-[58px]';

	let scrollPercentage = $state(0);
	let scrollbarWidth = $state(0);
	let scrollableDiv = $state<HTMLDivElement | undefined>(undefined);
	let isDragging = $state(false);
	let startX = 0;
	let scrollLeft = 0;

	function updateScrollbarWidth() {
		if (scrollableDiv) {
			scrollbarWidth = (scrollableDiv.clientWidth / scrollableDiv.scrollWidth) * 100;
		}
	}

	$effect(() => {
		updateScrollbarWidth();
	});

	onMount(() => {
		updateScrollbarWidth();

		window.addEventListener('resize', updateScrollbarWidth);

		return () => {
			window.removeEventListener('resize', updateScrollbarWidth);
		};
	});

	function handleScroll(e: Event) {
		const target = e.currentTarget as HTMLElement;
		const { scrollLeft, scrollWidth, clientWidth } = target;

		const maxScroll = scrollWidth - clientWidth;
		const availableWidth = 100 - scrollbarWidth;
		scrollPercentage = (scrollLeft / maxScroll) * availableWidth;
		scrollbarWidth = (clientWidth / scrollWidth) * 100;
	}

	function startDragging(e: MouseEvent) {
		if (!scrollableDiv) return;
		isDragging = true;
		startX = e.pageX;
		scrollLeft = scrollableDiv.scrollLeft;
	}

	function stopDragging() {
		isDragging = false;
	}

	function handleDrag(e: MouseEvent) {
		if (!isDragging || !scrollableDiv) return;
		e.preventDefault();

		// Calculate the total draggable width (container width minus scrollbar width)
		const containerWidth = scrollableDiv.clientWidth;
		const draggableWidth = containerWidth * (1 - scrollbarWidth / 100);

		// Calculate how far we can scroll
		const maxScroll = scrollableDiv.scrollWidth - containerWidth;

		// Calculate the movement ratio
		const ratio = maxScroll / draggableWidth;

		// Calculate distance moved
		const deltaX = e.pageX - startX;

		// Apply the scroll
		scrollableDiv.scrollLeft = scrollLeft + deltaX * ratio;
	}

	let isTimelineOpen = $state(true);
	let isMetadataOpen = $state(true);
</script>

<div class="flex flex-col">
	<CollapsibleSection title="Current Job" bind:open={isMetadataOpen}>
		{#snippet headerContentRight()}
			<LogsDownloadButton />
		{/snippet}
		{#snippet collapsibleContent()}
			<div class="grid grid-cols-2 gap-4">
				<div class="border rounded-md">
					<Table density="compact">
						<TableBody>
							<TableRow>
								<TableCell class="flex justify-between">
									<span>Job Name</span>
									<span>daily_sales_report</span>
								</TableCell>
							</TableRow>
							<TableRow>
								<TableCell class="flex justify-between">
									<span>Status</span>
									<span>Completed</span>
								</TableCell>
							</TableRow>
							<TableRow>
								<TableCell class="flex justify-between">
									<span>Start Date & Time</span>
									<span>Mar 20, 2024 09:15 AM</span>
								</TableCell>
							</TableRow>
						</TableBody>
					</Table>
				</div>
				<div class="border rounded-md">
					<Table density="compact">
						<TableBody>
							<TableRow>
								<TableCell class="flex justify-between">
									<span>Run By</span>
									<span>john.doe@company.com</span>
								</TableCell>
							</TableRow>
							<TableRow>
								<TableCell class="flex justify-between">
									<span>Execution Type</span>
									<span>Scheduled</span>
								</TableCell>
							</TableRow>
							<TableRow>
								<TableCell class="flex justify-between">
									<span>Job Type</span>
									<span>ETL Pipeline</span>
								</TableCell>
							</TableRow>
						</TableBody>
					</Table>
				</div>
			</div>
		{/snippet}
	</CollapsibleSection>

	<Separator fullWidthExtend={true} wide={true} />

	<CollapsibleSection title="Timeline" bind:open={isTimelineOpen}>
		{#snippet collapsibleContent()}
			<div class="flex flex-col gap-2">
				<div class="grid h-10">
					<!-- Overview chart content -->
					<div class="[grid-area:1/1] w-full">
						{#if $rows.length > 0}
							{@const firstJob = ($rows[0] as unknown as DataBodyRow<JobTreeNode>).original}
							<JobOverviewChart job={firstJob} dates={data.dates} {getDaysInRange} {formatDate} />
						{/if}
					</div>

					<!-- Scrollbar overlay -->
					<div
						class="bg-neutral-500 opacity-45 cursor-grab active:cursor-grabbing [grid-area:1/1]"
						style="width: {scrollbarWidth}%; margin-left: {scrollPercentage}%;"
						onmousedown={startDragging}
						role="scrollbar"
						aria-orientation="horizontal"
						aria-controls={scrollableDiv?.id || 'table-scroll'}
						aria-valuenow={scrollPercentage}
						tabindex="0"
					></div>
				</div>

				<div class="border-t">
					<Table bind:scrollContainer={scrollableDiv} hideScrollbar on:scroll={handleScroll}>
						<TableHeader>
							<TableRow class="border-none">
								<TableHead
									class={`min-w-[250px] ${tableHeadClass} ${stickyCellClass} text-neutral-900`}
									>Job Name</TableHead
								>
								{#each data.dates as date}
									<TableHead class={`min-w-[62px] ${tableHeadClass} text-xs text-neutral-900`}>
										{formatDate(date)}
									</TableHead>
								{/each}
							</TableRow>
						</TableHeader>
						<TableBody>
							{#each $rows as row (row.id)}
								<TableRow class="border-none">
									<TableCell class={`${tableCellClass} ${stickyCellClass}`}>
										<Render of={row.cells[0].render()} />
									</TableCell>
									{#each (row as unknown as DataBodyRow<JobTreeNode>).original.runs as run}
										{@const daysInRange = getDaysInRange(run.start, run.end, data.dates)}
										<TableCell colspan={daysInRange} class={`${tableCellClass} bg-neutral-100`}>
											<StatusCell status={run.status} startDate={run.start} endDate={run.end} />
										</TableCell>
									{/each}
								</TableRow>
							{/each}
						</TableBody>
					</Table>
				</div>
			</div>
		{/snippet}
	</CollapsibleSection>
</div>

<svelte:window on:mousemove={handleDrag} on:mouseup={stopDragging} on:mouseleave={stopDragging} />

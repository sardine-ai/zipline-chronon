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
	import { type JobTreeNode } from '$lib/job/tree-builder/tree-builder';
	import { onMount } from 'svelte';
	import { Separator } from '$lib/components/ui/separator';
	import JobOverviewChart from '$lib/components/JobOverviewChart.svelte';
	import { Button } from '$lib/components/ui/button';
	import IconArrowDownOnSquare16Solid from '~icons/heroicons/arrow-down-on-square-16-solid';
	import MetadataTable from '$lib/components/MetadataTable/MetadataTable.svelte';
	import MetadataTableSection from '$lib/components/MetadataTable/MetadataTableSection.svelte';
	import MetadataTableRow from '$lib/components/MetadataTable/MetadataTableRow.svelte';
	import { format, PeriodType } from '@layerstack/utils';
	import IconInformationCircle from '~icons/heroicons/information-circle';
	import StatusLegend from '$lib/components/StatusLegend.svelte';
	import { Popover, PopoverContent, PopoverTrigger } from '$lib/components/ui/popover';

	let { data } = $props();

	const jobsData = readable<JobTreeNode[]>(data.jobTree);

	function generateRootExpandedIds(data: JobTreeNode[]): Record<string, boolean> {
		return Object.fromEntries(data.map((_, index) => [index.toString(), true]));
	}

	const table = createTable(jobsData, {
		sub: addSubRows({
			children: 'children'
		}),
		/*
		Row IDs are formatted as follows:
		Root level: "0", "1", "2", etc.
		Children use > as separator: "0>0", "0>1", "1>0", "1>1", etc.
		Grandchildren: "0>0>0", "0>0>1", "0>1>0", "0>1>1", etc.
		*/
		expand: addExpandedRows({
			initialExpandedIds: generateRootExpandedIds(data.jobTree)
		})
	});

	const columns = table.createColumns([
		table.display({
			id: 'job-structure',
			header: 'Job structure',
			cell: ({ row }, { pluginStates }) => {
				const { isExpanded, canExpand, isAllSubRowsExpanded } =
					pluginStates.expand.getRowState(row);
				const original = (row as DataBodyRow<JobTreeNode>).original;
				const jobId = original.row;
				const node = original.node;
				const conf = original.conf;

				return createRender(ExpandableCell as unknown as typeof SvelteComponent, {
					depth: row.depth,
					isExpanded,
					canExpand,
					isAllSubRowsExpanded,
					name: jobId,
					node,
					conf
				});
			}
		})
	]);

	const { rows } = table.createViewModel(columns);

	function getDaysInRange(start: string, end: string, availableDates: string[]): number {
		return availableDates.filter((date) => date >= start && date <= end).length;
	}

	function formatDate(dateString: string): string {
		const formatted = format(dateString, PeriodType.Custom, {
			custom: {
				month: 'short',
				day: 'numeric',
				year: 'numeric'
			}
		});
		const [monthDay, year] = formatted.split(', ');
		return `${monthDay}\n${year}`;
	}

	const stickyCellClass = 'sticky left-0 bg-neutral-100 z-30';
	const tableHeadClass = 'h-[42px]';
	const tableCellClass = 'p-0 border-0 h-[48px]';

	let scrollPercentage = $state(0);
	let scrollbarWidth = $state(0);
	let scrollableDiv = $state<HTMLDivElement | undefined>(undefined);
	let isDragging = $state(false);
	let startX = 0;
	let scrollLeft = 0;

	let jobStructureHead = $state<HTMLElement | undefined>(undefined);

	function calculateScrollbarWidth(containerWidth: number, scrollWidth: number) {
		if (!jobStructureHead) return 0;
		const fixedColumnWidth = jobStructureHead.getBoundingClientRect().width;
		const scrollableWidth = scrollWidth - fixedColumnWidth;
		const visibleWidth = containerWidth - fixedColumnWidth;
		return (visibleWidth / scrollableWidth) * 100;
	}

	function updateScrollbarWidth() {
		if (scrollableDiv) {
			scrollbarWidth = calculateScrollbarWidth(
				scrollableDiv.clientWidth,
				scrollableDiv.scrollWidth
			);
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

		if (!jobStructureHead) return;

		const maxScroll = scrollWidth - clientWidth;
		const availableWidth = 100 - scrollbarWidth;
		scrollPercentage = (scrollLeft / maxScroll) * availableWidth;
		scrollbarWidth = calculateScrollbarWidth(clientWidth, scrollWidth);
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
	<CollapsibleSection title="Current job" bind:open={isMetadataOpen} class="mt-5 mb-6">
		{#snippet headerContentRight()}
			<Button variant="outline" size="md" class="text-small! text-muted-foreground">
				<IconArrowDownOnSquare16Solid class="mr-2 h-4 w-4" />
				Download Logs
			</Button>
		{/snippet}
		{#snippet collapsibleContent()}
			<!-- todo: use real data from API -->
			<MetadataTable columns={2}>
				<MetadataTableSection>
					<MetadataTableRow label="Job ID" value="chargeback_v1_adhoc_2024-01-03__2024-01-07" />
					<MetadataTableRow label="Status" value="Running" />
					<MetadataTableRow label="Start date & time" value="Oct 08, 2024 4:10pm" />
				</MetadataTableSection>
				<MetadataTableSection>
					<MetadataTableRow label="Run by" value="Nikhil Simha" />
					<MetadataTableRow label="Adhoc / Scheduled" value="Adhoc" />
					<MetadataTableRow label="Type" value="Batch upload" />
				</MetadataTableSection>
			</MetadataTable>
		{/snippet}
	</CollapsibleSection>

	<Separator fullWidthExtend={true} wide={true} />

	<CollapsibleSection title="Timeline" bind:open={isTimelineOpen} class="mt-7">
		{#snippet headerContentRight()}
			<Popover>
				<PopoverTrigger>
					<Button variant="ghost" class="text-muted-foreground flex items-center gap-1">
						<IconInformationCircle class="h-4 w-4" />
						<span class="text-sm">Status Legend</span>
					</Button>
				</PopoverTrigger>
				<PopoverContent side="bottom" align="end" class="p-4 w-auto">
					<StatusLegend />
				</PopoverContent>
			</Popover>
		{/snippet}
		{#snippet collapsibleContent()}
			<div class="flex flex-col gap-2 pt-2 border-t">
				<div class="grid h-10">
					<!-- Overview chart content -->
					<div class="[grid-area:1/1] w-full">
						{#if $rows.length > 0}
							{@const firstJob = ($rows[0] as unknown as DataBodyRow<JobTreeNode>).original}
							<JobOverviewChart job={firstJob} dates={data.dates} {getDaysInRange} />
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
									bind:element={jobStructureHead}
									class={`min-w-[250px] ${tableHeadClass} ${stickyCellClass} text-neutral-900 text-regular px-0 pb-2`}
								>
									Job structure
								</TableHead>
								{#each data.dates as date}
									<TableHead
										class={`min-w-[62px] ${tableHeadClass} text-xs text-neutral-700 px-2 border-b`}
									>
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
									{#each (row as unknown as DataBodyRow<JobTreeNode>).original.jobTracker?.tasksByDate?.values() ?? [] as task}
										{@const daysInRange = getDaysInRange(
											task.dateRange?.startDate ?? '',
											task.dateRange?.endDate ?? '',
											data.dates
										)}
										<TableCell colspan={daysInRange} class={`${tableCellClass} bg-neutral-100`}>
											<StatusCell status={task.status} endDate={task.dateRange?.endDate ?? ''} />
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

<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { cubicOut } from 'svelte/easing';
	import {
		Chart,
		Dagre,
		Group,
		Html,
		Spline,
		Svg,
		Tooltip,
		ancestors,
		descendants
	} from 'layerchart';
	import { curveBumpX, index } from 'd3';
	import { type Node } from '@dagrejs/dagre';

	import TransformControls from '$lib/components/charts/TransformControls.svelte';
	import { LogicalType, type IJoin, type INodeInfo, type INodeKey } from '$src/lib/types/codegen';
	import {
		getLogicalNodeConfig,
		getLogicalNodeType,
		isStreaming,
		type CombinedLogicalNode
	} from '$src/lib/types/LogicalNode.js';
	import { cn } from '$src/lib/utils';
	import { Dialog, DialogContent, DialogHeader } from '$lib/components/ui/dialog';
	import { tooltipProps } from '$src/lib/components/charts/common.js';
	import { Inspect } from 'svelte-inspect-value';
	import { Tabs, TabsContent, TabsList, TabsTrigger } from '$src/lib/components/ui/tabs';
	import { isMacOS } from '$src/lib/util/browser.js';
	import { Badge } from '$src/lib/components/ui/badge/index.js';
	import ConfProperties from './ConfProperties.svelte';
	import CollapsibleSection from '$src/lib/components/CollapsibleSection.svelte';
	import { Separator } from '$lib/components/ui/separator';

	type DagreData = ComponentProps<Dagre>['data'];
	type CustomNode = Node & { id: string; key: INodeKey; value: INodeInfo };

	const { data } = $props();

	const { connections, infoMap } = data.lineage.nodeGraph ?? {};

	const nodes: DagreData['nodes'] = $derived(
		Array.from(infoMap?.entries() ?? []).map(([key, value]) => ({
			id: key.name!,
			label: { key, value }
		})) ?? []
	);

	function getEdges(node: INodeKey): DagreData['edges'] {
		const parents = connections?.get(node)?.parents;

		return (
			parents?.flatMap((p) => {
				return [{ source: p.name!, target: node.name! }, ...getEdges(p)];
			}) ?? []
		);
	}
	const edges = getEdges(data.lineage.mainNode!);

	const chartData = $derived({ nodes, edges });

	let selectedNode = $state<CustomNode | null>(null);
	let hoveredNode = $state<CustomNode | null>(null);
	let graph = $state<ComponentProps<Dagre>['graph'] | undefined>(undefined);
	let hideTooltip = $state(false);
	let isDetailsOpen = $state(true);

	/** Determine if node should be faded based on if node is upstream or downstream from hoveredNode */
	function fadeNode(node: CustomNode) {
		if (graph && hoveredNode) {
			return !(node.id === hoveredNode.id || isUpstream(node) || isDownstream(node));
		} else {
			return false;
		}
	}
	/** Determine if link should be faded based on if source/target are upstream or downstream from hoveredNode */
	function fadeEdge(edge: { source: CustomNode; target: CustomNode }) {
		if (graph && hoveredNode) {
			const upstreamEdge =
				isUpstream(edge.source as CustomNode) &&
				(isUpstream(edge.target as CustomNode) || edge.target.id === hoveredNode.id);
			const downstreamEdge =
				(isDownstream(edge.source as CustomNode) || edge.source.id === hoveredNode.id) &&
				isDownstream(edge.target as CustomNode);

			return !upstreamEdge && !downstreamEdge;
		} else {
			return false;
		}
	}

	function isUpstream(node: CustomNode) {
		if (graph && hoveredNode) {
			const upstream = ancestors(graph, hoveredNode.id) as unknown as string[];
			return upstream?.includes(node.id) ?? false;
		} else {
			return false;
		}
	}
	function isDownstream(node: CustomNode) {
		if (graph && hoveredNode) {
			const downstream = descendants(graph, hoveredNode.id) as unknown as string[];
			return downstream?.includes(node.id) ?? false;
		} else {
			return false;
		}
	}
</script>

<CollapsibleSection title="Details" bind:open={isDetailsOpen} class="mt-5 mb-6">
	{#snippet collapsibleContent()}
		<ConfProperties conf={data.join} />
	{/snippet}
</CollapsibleSection>

<Separator fullWidthExtend={true} wide={true} />

<div class="flex gap-2 py-4">
	<div class="h-[600px] flex-1 border rounded">
		<Chart
			data={chartData}
			transform={{
				mode: 'canvas',
				initialScrollMode: 'scale',
				tweened: { duration: 800, easing: cubicOut }
			}}
			padding={{ top: 60, bottom: 20, left: 20, right: 20 }}
			let:tooltip
		>
			<TransformControls />

			<div class="relative overflow-hidden w-full h-full">
				<Dagre
					data={chartData}
					direction="left-right"
					nodeWidth={300}
					nodeSeparation={100}
					rankSeparation={100}
					edgeSeparation={20}
					bind:graph
					let:nodes
					let:edges
				>
					<!-- Adjust edges to be relative to the nodes -->
					{@const nodesById = index(nodes, (n) => (n as CustomNode).id)}
					{@const nodeEdges = edges.map((e) => {
						const source = nodesById.get(e.v) as CustomNode;
						const target = nodesById.get(e.w) as CustomNode;

						const sourceType = getLogicalNodeType(source.value.conf as CombinedLogicalNode);
						const targetType = getLogicalNodeType(target.value.conf as CombinedLogicalNode);

						const sourcePoint = {
							x: source.x + source.width / 2,
							y: source.y
						};
						const targetPoint = {
							x: target.x - target.width / 2,
							y: target.y
						};

						let points: Array<{ x: number; y: number }> = [];
						if (sourceType === LogicalType.TABULAR_DATA && targetType === LogicalType.JOIN) {
							// If `join.left` use dagre-defined edge to route around other nodes (typically at the top) but to common point on source/target
							points = e.points;
							points[0] = sourcePoint;
							points[points.length - 1] = targetPoint;
						} else {
							points = [sourcePoint, targetPoint];
						}

						return {
							...e,
							source,
							target,
							points
						};
					})}

					<Html>
						<div class="nodes">
							{#each nodes as _node (_node.label)}
								{@const node = _node as CustomNode}
								{@const [namespace, ...nameParts] = node.label?.split('.') ?? []}
								{@const config = getLogicalNodeConfig(node)}
								{@const Icon = config?.icon}
								<Group
									class={cn(
										'bg-neutral-300 border border-neutral-400 rounded-md',
										'cursor-pointer hover:outline outline-surface-content/20 outline-offset-0 hover:outline-offset-1 transition-all',
										fadeNode(node) && 'opacity-20'
									)}
									x={node.x - node.width / 2}
									y={node.y - node.height / 2}
									style="width:{node.width}px; height:{node.height}px"
									onclick={() => {
										selectedNode = node;
									}}
									onpointermove={(e) => {
										hoveredNode = node;
										tooltip.show(e, node);
									}}
									onpointerleave={() => {
										hoveredNode = null;
										tooltip.hide();
									}}
								>
									<div class="h-full px-2 grid grid-cols-[auto_1fr] gap-3 items-center">
										<div
											style:--color={config.color}
											class="bg-[hsl(var(--color)/5%)] border border-[hsl(var(--color))] text-[hsl(var(--color))] w-8 h-8 rounded flex items-center justify-center"
										>
											<Icon />
										</div>
										<div class="align-middle truncate">
											<div class="text-xs text-surface-content/50">
												{namespace}
											</div>
											<div class="text-sm text-surface-content truncate">
												{nameParts.join('.')}
											</div>
										</div>
									</div>
								</Group>
							{/each}
						</div>
					</Html>

					<Svg pointerEvents={false}>
						<g class="edges">
							{#each nodeEdges as edge (edge.v + '-' + edge.w)}
								<Spline
									data={edge.points}
									x="x"
									y="y"
									class={cn(
										'stroke-neutral-500 stroke-[1.5] [stroke-dasharray:5_5]',
										fadeEdge(edge as unknown as { source: CustomNode; target: CustomNode }) &&
											'opacity-20'
									)}
									tweened
									curve={curveBumpX}
									markerStart="circle"
									markerEnd="circle"
								/>

								<Spline
									data={edge.points}
									x="x"
									y="y"
									class={cn(
										isStreaming(edge.source.value.conf as CombinedLogicalNode)
											? 'stroke-blue-500 stroke-[2] [stroke-dasharray:10_10] [stroke-dashoffset:20] animate-dashoffset-2x'
											: 'stroke-purple-500 stroke-[2] [stroke-dasharray:30_100] [stroke-dashoffset:130] animate-dashoffset-0.5x',
										fadeEdge(edge as unknown as { source: CustomNode; target: CustomNode }) &&
											'opacity-0'
									)}
									tweened
									curve={curveBumpX}
								/>
							{/each}
						</g>
					</Svg>
				</Dagre>
			</div>

			{#if !hideTooltip}
				<Tooltip.Root {...tooltipProps.root} contained="window" xOffset={0} yOffset={30} let:data>
					<Tooltip.List {...tooltipProps.list}>
						<!-- MetaData -->
						{#each ['name' /*, 'team', 'outputNamespace', 'offlineSchedule', 'online', 'production'*/] as prop}
							{#if data.value.conf.metaData?.[prop] !== undefined}
								<Tooltip.Item
									{...tooltipProps.item}
									label={prop}
									value={data.value.conf.metaData?.[prop]}
									valueAlign="left"
								/>
							{/if}
						{/each}

						<!-- GroupBy -->
						{#if data.value.conf.keyColumns}
							<Tooltip.Item
								{...tooltipProps.item}
								label="keyColumns"
								classes={{ label: cn(tooltipProps.item.classes?.label, 'self-start') }}
								valueAlign="left"
							>
								{#each data.value.conf.keyColumns as keyColumn}
									<div>{keyColumn}</div>
								{/each}
							</Tooltip.Item>
						{/if}

						<!-- EntitySource -->

						{#each ['table', 'snapshotTable', 'mutationTable', 'mutationTopic'] as prop}
							{#if data.value.conf[prop] !== undefined}
								<Tooltip.Item
									{...tooltipProps.item}
									label={prop}
									value={data.value.conf[prop]}
									valueAlign="left"
								/>
							{/if}
						{/each}

						{#if data.value.conf.query}
							<Tooltip.Item
								{...tooltipProps.item}
								label="selects"
								classes={{ label: cn(tooltipProps.item.classes?.label, 'self-start') }}
								valueAlign="left"
							>
								{#each Object.entries(data.value.conf.query.selects) as [key, _]}
									<div>{key}</div>
								{/each}
							</Tooltip.Item>
						{/if}
					</Tooltip.List>

					<div class="text-foreground text-xs px-3 py-2 border-t">
						<Badge variant="key">{isMacOS() ? '⌘' : 'Ctrl'}</Badge> to hide tooltip
					</div>
				</Tooltip.Root>
			{/if}
		</Chart>
	</div>
</div>

<Dialog open={selectedNode != null} onOpenChange={() => (selectedNode = null)}>
	{#if selectedNode}
		<Tabs value="overview" class="w-full">
			<DialogContent class="max-w-[85vw] h-[95vh] flex flex-col p-0">
				<DialogHeader class="pt-4 px-7 border-b">
					{@const [namespace, ...nameParts] = selectedNode.label?.split('.') ?? []}
					{@const config = getLogicalNodeConfig(selectedNode)}
					{@const Icon = config?.icon}

					<div class="h-full px-2 grid grid-cols-[auto_1fr] gap-3 items-center pb-4">
						<div
							style:--color={config.color}
							class="bg-[hsl(var(--color)/5%)] border border-[hsl(var(--color))] text-[hsl(var(--color))] w-8 h-8 rounded flex items-center justify-center"
						>
							<Icon />
						</div>
						<div class="align-middle truncate">
							<div class="text-xs text-surface-content/50">
								{namespace}
							</div>
							<div class="text-sm text-surface-content truncate">
								{nameParts.join('.')}
							</div>
						</div>
					</div>

					<TabsList class="justify-start">
						<TabsTrigger value="overview">Overview</TabsTrigger>
						<TabsTrigger value="details">Details</TabsTrigger>
					</TabsList>
				</DialogHeader>

				<TabsContent value="overview" class="overflow-auto px-7">
					<ConfProperties conf={selectedNode.value.conf as IJoin} includeUpstream />
				</TabsContent>

				<TabsContent value="details" class="overflow-auto px-7">
					<div class="border py-2 px-4 rounded-md">
						<Inspect
							name={selectedNode.key.name}
							value={selectedNode.value}
							showLength={false}
							showTypes={false}
							showPreview={false}
							showTools={false}
							expandLevel={7}
							borderless
							theme=""
							--base00="hsl(var(--background))"
							--base01="hsl(var(--muted) / 20%)"
							--base02="hsl(var(--primary-500))"
							--base03="hsl(var(--border))"
							--base05="hsl(var(--foreground))"
							--base08="hsl(var(--primary-700))"
						/>
					</div>
				</TabsContent>
			</DialogContent>
		</Tabs>
	{/if}
</Dialog>

<svelte:window
	onkeydown={(e) => {
		if (isMacOS() ? e.metaKey : e.ctrlKey) {
			hideTooltip = true;
		}
	}}
	onkeyup={(e) => {
		if (isMacOS() ? !e.metaKey : !e.ctrlKey) {
			hideTooltip = false;
		}
	}}
/>

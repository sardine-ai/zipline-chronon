<script lang="ts">
	import { ExpansionPanel } from 'svelte-ux';

	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$src/lib/components/ui/table/index.js';
	import {
		TimeUnit,
		type IAggregationArgs,
		type ISourceArgs,
		type IMetaDataArgs,
		Operation,
		ModelType,
		DataKind
	} from '$src/lib/types/codegen';
	import { keys } from '@layerstack/utils';
	import Self from './EntityProperties.svelte';
	import TrueFalseBadge from '$src/lib/components/TrueFalseBadge.svelte';
	import { getEntityConfig, type EntityData } from '$src/lib/types/Entity/Entity';

	const {
		entity,
		metaDataLabel = 'MetaData',
		includeUpstream = false
	}: {
		entity: EntityData;
		metaDataLabel?: string;
		includeUpstream?: boolean;
	} = $props();

	const METADATA_PROPERTIES = [
		'name',
		'team',
		'outputNamespace',
		'offlineSchedule',
		'online',
		'production'
	] satisfies (keyof IMetaDataArgs)[];

	const AGGREGATION_PROPERTIES = [
		'inputColumn',
		'operation',
		'argMap',
		'windows'
	] satisfies (keyof IAggregationArgs)[];

	type SourceProperties = keyof NonNullable<
		ISourceArgs['entities'] & ISourceArgs['events'] & ISourceArgs['joinSource']
	>;
	const SOURCE_PROPERTIES = [
		'table',
		'snapshotTable',
		'mutationTable',
		'mutationTopic'
	] satisfies Array<SourceProperties>;
</script>

<div class="grid gap-4 text-sm overflow-auto">
	{#if entity?.metaData}
		<div>
			{#if metaDataLabel}
				<div class="font-semibold py-2">{metaDataLabel}</div>
			{/if}
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each METADATA_PROPERTIES as prop}
							{#if prop in entity.metaData}
								<TableRow class="border-neutral-400">
									<TableCell width="200px">
										<span class="text-muted-foreground">{prop}</span>
									</TableCell>
									<TableCell>
										{#if prop === 'online' || prop === 'production'}
											<TrueFalseBadge isTrue={entity.metaData[prop] ?? false} />
										{:else}
											{entity.metaData[prop]}
										{/if}
									</TableCell>
								</TableRow>
							{/if}
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if 'modelType' in entity}
		<div>
			<div class="font-semibold py-2">Model</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						<TableRow class="border-neutral-400">
							<TableCell width="200px">
								<span class="text-muted-foreground">modelType</span>
							</TableCell>
							<TableCell>
								{ModelType[entity.modelType ?? 0]}
							</TableCell>
						</TableRow>

						<TableRow class="border-neutral-400">
							<TableCell width="200px">
								<span class="text-muted-foreground">modelParams</span>
							</TableCell>
							<TableCell>
								{#each Object.entries(entity.modelParams ?? {}) as [key, value]}
									<div>
										<span class="text-muted-foreground">{key}:</span>
										{value}
									</div>
								{/each}
							</TableCell>
						</TableRow>

						<TableRow class="border-neutral-400">
							<TableCell width="200px">
								<span class="text-muted-foreground">outputSchema</span>
							</TableCell>
							<TableCell>
								<div>
									<span class="text-muted-foreground">name:</span>
									{entity.outputSchema?.name}
								</div>
								<div>
									<span class="text-muted-foreground">kind:</span>
									{DataKind[entity.outputSchema?.kind ?? 0]}
								</div>
								<div>
									<span class="text-muted-foreground">params:</span>
									{entity.outputSchema?.params?.map((p) => `${p.name}: ${p.dataType}`).join(', ')}
								</div>
							</TableCell>
						</TableRow>
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if (entity?.left || entity?.source) && includeUpstream}
		{@const source = entity.left ?? entity.source}
		{#if source}
			{@const entity = (source.entities ?? source.events ?? source.joinSource?.join) as EntityData}
			{@const config = getEntityConfig(entity)}
			{@const Icon = config.icon}
			{@const label =
				entity?.table ?? entity?.snapshotTable ?? entity?.metaData?.name ?? 'Unknown source'}
			<div>
				<div class="font-semibold py-2">
					{entity.left ? 'Left' : 'Source'} ({source.entities
						? 'entity'
						: source.events
							? 'event'
							: 'join'})
				</div>

				<div class="border rounded-md">
					{#if entity}
						<ExpansionPanel popout={false} classes={{ root: 'bg-transparent' }}>
							<div slot="trigger" class="flex-1 p-3 flex items-center gap-3">
								{#if Icon}
									<div
										style:--color={config.color}
										class="text-[hsl(var(--color))] w-4 h-4 flex items-center justify-center"
									>
										<Icon />
									</div>
								{/if}
								<span>
									{label}
								</span>
							</div>
							<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
								<Self entity={entity as typeof entity} {includeUpstream} />
							</div>
						</ExpansionPanel>
					{/if}
				</div>
			</div>
		{/if}
	{/if}

	{#if entity?.rowIds}
		<div>
			<div class="font-semibold py-2">Row IDs</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each entity?.rowIds as rowId}
							<TableRow class="border-neutral-400">
								<TableCell>
									{rowId}
								</TableCell>
							</TableRow>
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if entity?.keyColumns}
		<div>
			<div class="font-semibold py-2">Key Columns</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each entity?.keyColumns as keyColumn}
							<TableRow class="border-neutral-400">
								<TableCell>
									{keyColumn}
								</TableCell>
							</TableRow>
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if entity?.aggregations}
		<div>
			<div class="font-semibold py-2">Aggregations</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableHeader>
						<TableRow>
							<TableHead>Input Column</TableHead>
							<TableHead>Operation</TableHead>
							<TableHead>Arg Map</TableHead>
							<TableHead>Windows</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{#each entity.aggregations as agg}
							<TableRow class="border-neutral-400">
								{#each AGGREGATION_PROPERTIES as prop}
									<TableCell>
										{#if prop === 'operation'}
											{Operation[agg.operation ?? 0]}
										{:else if prop === 'windows'}
											{agg.windows
												?.map((w) => `${w.length} ${TimeUnit[w.timeUnit ?? 0].toLowerCase()}`)
												.join(', ')}
										{:else if prop === 'argMap'}
											{#each Object.entries(agg?.[prop] ?? {}) as [key, value]}
												<div>
													<span class="text-muted-foreground">{key}:</span>
													{value}
												</div>
											{/each}
										{:else}
											{agg[prop]}
										{/if}
									</TableCell>
								{/each}
							</TableRow>
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	<!-- Sources -->

	<!-- Source with chained join -->
	{#if entity?.join}
		<Self entity={entity.join} metaDataLabel="Join" {includeUpstream} />
	{/if}

	{#if keys(entity).some((key) => {
		// @ts-expect-error: find way to do this without TS complaining
		return SOURCE_PROPERTIES.includes(key);
	})}
		<div>
			<div class="font-semibold py-2">Tables & Events</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each SOURCE_PROPERTIES as prop}
							{#if prop in entity}
								<TableRow class="border-neutral-400">
									<TableCell width="200px">
										<span class="text-muted-foreground">{prop}</span>
									</TableCell>
									<TableCell>
										{entity[prop]}
									</TableCell>
								</TableRow>
							{/if}
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if entity?.query}
		<div>
			<div class="font-semibold py-2">Query</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each ['selects', 'wheres', 'startPartition', 'endPartition', 'timeColumn', 'setup', 'mutationTimeColumn', 'reversalColumn'] as prop}
							{#if prop in entity.query}
								<TableRow class="border-neutral-400">
									<TableCell width="200px" class="text-muted-foreground">
										{prop}
									</TableCell>
									<TableCell>
										{#if prop === 'selects'}
											{#each Object.entries(entity.query.selects ?? {}) as [key, value]}
												<div>
													<span class="text-muted-foreground">{key}:</span>
													{value}
												</div>
											{/each}
										{:else if typeof entity.query === 'object'}
											{entity.query[prop]}
										{:else}
											{entity.query}
										{/if}
									</TableCell>
								</TableRow>
							{/if}
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if includeUpstream}
		{#if entity?.joinParts}
			<div>
				<div class="font-semibold py-2">Join Parts</div>
				<div class="border rounded-md">
					{#each entity?.joinParts ?? [] as joinPart}
						{#if joinPart.groupBy}
							{@const entity = joinPart.groupBy}
							{@const config = getEntityConfig(entity as EntityData)}
							{@const Icon = config.icon}
							<ExpansionPanel popout={false} classes={{ root: 'bg-transparent' }}>
								<div slot="trigger" class="flex-1 p-3 flex items-center gap-3">
									{#if Icon}
										<div
											style:--color={config.color}
											class="text-[hsl(var(--color))] w-4 h-4 flex items-center justify-center"
										>
											<Icon />
										</div>
									{/if}
									<span>
										{entity.metaData?.name ?? 'Unknown groupBy'}
									</span>
								</div>
								<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
									<Self entity={joinPart.groupBy} {includeUpstream} />
								</div>
							</ExpansionPanel>
						{/if}
					{/each}
				</div>
			</div>
		{/if}

		{#if entity?.sources}
			<div>
				<div class="font-semibold py-2">Sources</div>
				<div class="border rounded-md">
					{#each entity?.sources ?? [] as source}
						{@const entity = (source.entities ??
							source.events ??
							source.joinSource?.join) as EntityData}
						{@const config = getEntityConfig(entity)}
						{@const Icon = config.icon}
						{@const label =
							entity?.table ?? entity?.snapshotTable ?? entity?.metaData?.name ?? 'Unknown source'}
						{#if entity}
							<ExpansionPanel popout={false} classes={{ root: 'bg-transparent' }}>
								<div slot="trigger" class="flex-1 p-3 flex items-center gap-3">
									{#if Icon}
										<div
											style:--color={config.color}
											class="text-[hsl(var(--color))] w-4 h-4 flex items-center justify-center"
										>
											<Icon />
										</div>
									{/if}
									<span>
										{label}
									</span>
								</div>
								<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
									<Self entity={entity as typeof entity} {includeUpstream} />
								</div>
							</ExpansionPanel>
						{/if}
					{/each}
				</div>
			</div>
		{/if}
	{/if}
</div>

<script lang="ts">
	import CollapsibleSection from '$src/lib/components/CollapsibleSection.svelte';
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
		type IAggregation,
		type IGroupBy,
		type IJoin,
		type IModel,
		type IStagingQuery,
		type ITabularData,
		type ISource,
		type IMetaData,
		type IJoinSource,
		Operation,
		ModelType,
		DataKind
	} from '$src/lib/types/codegen';
	import { keys } from '@layerstack/utils';
	import Self from './ConfProperties.svelte';
	import TrueFalseBadge from '$src/lib/components/TrueFalseBadge.svelte';

	const {
		conf,
		metaDataLabel = 'MetaData',
		includeUpstream = false
	}: {
		conf: IJoin &
			IGroupBy &
			IModel &
			IStagingQuery &
			ITabularData &
			IJoinSource &
			ISource['entities'] &
			ISource['events'] &
			ISource['joinSource'];
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
	] satisfies (keyof IMetaData)[];

	const AGGREGATION_PROPERTIES = [
		'inputColumn',
		'operation',
		'argMap',
		'windows'
	] satisfies (keyof IAggregation)[];

	type SourceProperties = keyof NonNullable<
		ISource['entities'] & ISource['events'] & ISource['joinSource']
	>;
	const SOURCE_PROPERTIES = [
		'table',
		'snapshotTable',
		'mutationTable',
		'mutationTopic'
	] satisfies Array<SourceProperties>;

	let isJoinPartsOpen = $state(false);
	let isSourcesOpen = $state(false);
</script>

<div class="grid gap-4 text-sm overflow-auto">
	{#if conf?.metaData}
		<div>
			{#if metaDataLabel}
				<div class="font-semibold py-2">{metaDataLabel}</div>
			{/if}
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each METADATA_PROPERTIES as prop}
							{#if prop in conf.metaData}
								<TableRow class="border-neutral-400">
									<TableCell width="200px">
										<span class="text-muted-foreground">{prop}</span>
									</TableCell>
									<TableCell>
										{#if prop === 'online' || prop === 'production'}
											<TrueFalseBadge isTrue={conf.metaData[prop] ?? false} />
										{:else}
											{conf.metaData[prop]}
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

	{#if 'modelType' in conf}
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
								{ModelType[conf.modelType ?? 0]}
							</TableCell>
						</TableRow>

						<TableRow class="border-neutral-400">
							<TableCell width="200px">
								<span class="text-muted-foreground">modelParams</span>
							</TableCell>
							<TableCell>
								{#each Object.entries(conf.modelParams ?? {}) as [key, value]}
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
									{conf.outputSchema?.name}
								</div>
								<div>
									<span class="text-muted-foreground">kind:</span>
									{DataKind[conf.outputSchema?.kind ?? 0]}
								</div>
								<div>
									<span class="text-muted-foreground">params:</span>
									{conf.outputSchema?.params?.map((p) => `${p.name}: ${p.dataType}`).join(', ')}
								</div>
							</TableCell>
						</TableRow>
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if (conf?.left || conf?.source) && includeUpstream}
		{@const source = conf.left ?? conf.source}
		{#if source}
			<div>
				<div class="font-semibold py-2">
					{conf.left ? 'Left' : 'Source'} ({source.entities
						? 'entity'
						: source.events
							? 'event'
							: 'join'})
				</div>
				<div class="border border-neutral-400 rounded-md">
					<Table density="compact">
						<TableBody>
							<TableRow class="border-neutral-400">
								<TableCell>
									<Self
										conf={(source.entities ?? source.events ?? source.joinSource) as typeof conf}
										{includeUpstream}
									/>
								</TableCell>
							</TableRow>
						</TableBody>
					</Table>
				</div>
			</div>
		{/if}
	{/if}

	{#if conf?.rowIds}
		<div>
			<div class="font-semibold py-2">Row IDs</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each conf?.rowIds as rowId}
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

	{#if conf?.keyColumns}
		<div>
			<div class="font-semibold py-2">Key Columns</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each conf?.keyColumns as keyColumn}
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

	{#if conf?.aggregations}
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
						{#each conf.aggregations as agg}
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
	{#if conf?.join}
		<Self conf={conf.join} metaDataLabel="Join" {includeUpstream} />
	{/if}

	{#if keys(conf).some((key) => {
		// @ts-expect-error: find way to do this without TS complaining
		return SOURCE_PROPERTIES.includes(key);
	})}
		<div>
			<div class="font-semibold py-2">Tables & Events</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each SOURCE_PROPERTIES as prop}
							{#if prop in conf}
								<TableRow class="border-neutral-400">
									<TableCell width="200px">
										<span class="text-muted-foreground">{prop}</span>
									</TableCell>
									<TableCell>
										{conf[prop]}
									</TableCell>
								</TableRow>
							{/if}
						{/each}
					</TableBody>
				</Table>
			</div>
		</div>
	{/if}

	{#if conf?.query}
		<div>
			<div class="font-semibold py-2">Query</div>
			<div class="border border-neutral-400 rounded-md">
				<Table density="compact">
					<TableBody>
						{#each ['selects', 'wheres', 'startPartition', 'endPartition', 'timeColumn', 'setup', 'mutationTimeColumn', 'reversalColumn'] as prop}
							{#if prop in conf.query}
								<TableRow class="border-neutral-400">
									<TableCell width="200px" class="text-muted-foreground">
										{prop}
									</TableCell>
									<TableCell>
										{#if prop === 'selects'}
											{#each Object.entries(conf.query.selects ?? {}) as [key, value]}
												<div>
													<span class="text-muted-foreground">{key}:</span>
													{value}
												</div>
											{/each}
										{:else if typeof conf.query === 'object'}
											{conf.query[prop]}
										{:else}
											{conf.query}
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
		{#if conf?.joinParts}
			<CollapsibleSection title="Join Parts" bind:open={isJoinPartsOpen}>
				{#snippet collapsibleContent()}
					<div class="ml-4 grid gap-3">
						{#each conf?.joinParts ?? [] as joinPart}
							{#if joinPart.groupBy}
								<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
									<Self conf={joinPart.groupBy} {includeUpstream} />
								</div>
							{/if}
						{/each}
					</div>
				{/snippet}
			</CollapsibleSection>
		{/if}

		{#if conf?.sources}
			<CollapsibleSection title="Sources" bind:open={isSourcesOpen}>
				{#snippet collapsibleContent()}
					<div class="ml-4 grid gap-3">
						{#each conf?.sources ?? [] as source}
							<!-- TODO: Fix "Type `IQuery` is not assignable to type `string`" and remove `as any` -->
							{#if source?.entities}
								<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
									<Self conf={source.entities as typeof conf} {includeUpstream} />
								</div>
							{/if}

							{#if source?.events}
								<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
									<Self conf={source.events as typeof conf} {includeUpstream} />
								</div>
							{/if}

							{#if source?.joinSource}
								<div class="border rounded-md px-4 py-2 bg-neutral-500/10">
									<Self conf={source.joinSource as typeof conf} {includeUpstream} />
								</div>
							{/if}
						{/each}
					</div>
				{/snippet}
			</CollapsibleSection>
		{/if}
	{/if}
</div>

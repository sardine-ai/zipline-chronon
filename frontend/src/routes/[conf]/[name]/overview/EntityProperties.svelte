<script lang="ts">
	import { ExpansionPanel, Table } from 'svelte-ux';
	import { keys } from '@layerstack/utils';
	import { tableCell } from '@layerstack/svelte-table';

	import {
		TimeUnit,
		type ISourceArgs,
		type IMetaDataArgs,
		Operation,
		ModelType,
		DataKind
	} from '$src/lib/types/codegen';

	import Self from './EntityProperties.svelte';
	import { getEntityConfig, type EntityData } from '$src/lib/types/Entity';
	import PropertyList from '$src/lib/components/PropertyList.svelte';

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
		'executionInfo',
		'online',
		'production'
	] satisfies (keyof IMetaDataArgs)[];

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

			<PropertyList
				items={METADATA_PROPERTIES.filter((p) => p in (entity.metaData ?? {})).map((p) => ({
					label: p,
					value:
						p === 'executionInfo' ? entity.metaData?.[p]?.['scheduleCron'] : entity.metaData?.[p],
					type: ['online', 'production'].includes(p) ? 'boolean' : 'string'
				}))}
			/>
		</div>
	{/if}

	{#if 'modelType' in entity}
		<div>
			<div class="font-semibold py-2">Model</div>
			<PropertyList
				items={[
					{ label: 'modelType', value: ModelType[entity.modelType ?? 0] },
					{ label: 'modelParams', value: entity.modelParams },
					{
						label: 'outputSchema',
						value: {
							name: entity.outputSchema?.name,
							kind: DataKind[entity.outputSchema?.kind ?? 0],
							params: entity.outputSchema?.params?.map((p) => `${p.name}: ${p.dataType}`)
						}
					}
				]}
			/>
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
			<PropertyList
				items={entity?.rowIds?.map((column) => ({
					value: column
				}))}
			/>
		</div>
	{/if}

	{#if entity?.keyColumns}
		<div>
			<div class="font-semibold py-2">Key Columns</div>
			<PropertyList
				items={entity?.keyColumns?.map((column) => ({
					value: column
				}))}
			/>
		</div>
	{/if}

	{#if entity?.aggregations}
		<div>
			<div class="font-semibold py-2">Aggregations</div>

			<div class="border border-neutral-400 rounded-md">
				<Table
					data={entity.aggregations}
					columns={[
						{
							name: 'inputColumn',
							header: 'Input Column'
						},
						{
							name: 'operation',
							header: 'Operation',
							value: (d) => Operation[d.operation ?? 0]
						},
						{
							name: 'argMap',
							header: 'Arg Map'
						},
						{
							name: 'windows',
							header: 'Windows',
							value: (d) =>
								d.windows
									?.map((w) => `${w.length} ${TimeUnit[w.timeUnit ?? 0].toLowerCase()}`)
									.join(', ')
						}
					]}
					classes={{
						table: 'text-sm',
						th: 'border-r border-b px-3 py-2 text-muted-foreground'
					}}
				>
					<tbody slot="data" let:columns let:data let:getCellValue let:getCellContent>
						{#each data ?? [] as rowData, rowIndex}
							<tr class="border-b last:border-b-0">
								{#each columns as column (column.name)}
									{@const value = getCellValue(column, rowData, rowIndex)}
									<td
										use:tableCell={{ column, rowData, rowIndex, tableData: data }}
										class="border-r px-3 py-1"
									>
										{#if column.name === 'argMap'}
											{#each Object.entries(value) as [key, v]}
												<div>
													<span class="text-muted-foreground">{key}:</span>
													{v}
												</div>
											{/each}
										{:else}
											{getCellContent(column, rowData, rowIndex)}
										{/if}
									</td>{/each}
							</tr>
						{/each}
					</tbody>
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
			<PropertyList
				items={SOURCE_PROPERTIES.filter((p) => p in entity).map((p) => ({
					label: p,
					value: entity[p]
				}))}
			/>
		</div>
	{/if}

	{#if entity?.query}
		<div>
			<div class="font-semibold py-2">Query</div>
			<PropertyList
				items={[
					'selects',
					'wheres',
					'startPartition',
					'endPartition',
					'timeColumn',
					'setup',
					'mutationTimeColumn',
					'reversalColumn'
				]
					.filter((p) => p in (entity.query ?? {}))
					.map((p) => ({
						label: p,
						value: typeof entity.query === 'object' ? entity.query[p] : null
					}))}
			/>
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

<script lang="ts">
	import type { IJoinArgs, IGroupByArgs, IModelArgs, IStagingQueryArgs } from '$lib/types/codegen';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import { type EntityConfig } from '$lib/types/Entity';

	import TrueFalseBadge from '$lib/components/TrueFalseBadge.svelte';

	const {
		entityConfig,
		items,
		basePath
	}: {
		entityConfig: EntityConfig;
		items: (IJoinArgs | IGroupByArgs | IModelArgs | IStagingQueryArgs)[];
		basePath: string;
	} = $props();
</script>

<Table>
	<TableHeader>
		<TableRow>
			<TableHead>Name</TableHead>
			<TableHead>Team</TableHead>
			<TableHead>Online</TableHead>
			<TableHead>Production</TableHead>
		</TableRow>
	</TableHeader>
	<TableBody>
		{#if items.length === 0}
			<TableRow>
				<TableCell>
					No {entityConfig.label.toLowerCase()} found.
				</TableCell>
			</TableRow>
		{:else}
			{#each items as item}
				<TableRow>
					<TableCell>
						<!-- todo: enable all items once we have data for them -->
						<a
							href={`${basePath}/${encodeURIComponent(item.metaData?.name ?? '')}`}
							class="hover:underline {item.metaData?.name !== 'risk.user_transactions.txn_join'
								? 'opacity-50'
								: ''}"
						>
							{item.metaData?.name}
						</a>
					</TableCell>
					<TableCell>{item.metaData?.team ?? ''}</TableCell>
					<TableCell>
						<TrueFalseBadge isTrue={item.metaData?.online ?? false} />
					</TableCell>
					<TableCell>
						<TrueFalseBadge isTrue={item.metaData?.production ?? false} />
					</TableCell>
				</TableRow>
			{/each}
		{/if}
	</TableBody>
</Table>

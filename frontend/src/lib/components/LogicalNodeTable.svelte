<script lang="ts">
	import type { IJoin, IGroupBy, IModel, IStagingQuery } from '$lib/types/codegen';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import ActionButtons from '$lib/components/ActionButtons.svelte';

	const {
		title,
		items,
		basePath
	}: { title: string; items: (IJoin | IGroupBy | IModel | IStagingQuery)[]; basePath: string } =
		$props();
</script>

<PageHeader {title} />

<div class="w-full">
	<ActionButtons class="mb-4" />
</div>

<Separator fullWidthExtend={true} />

<Table>
	<TableHeader>
		<TableRow>
			<TableHead>{title}</TableHead>
		</TableRow>
	</TableHeader>
	<TableBody>
		{#if items.length === 0}
			<TableRow>
				<TableCell>
					No {title.toLowerCase()} found.
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
				</TableRow>
			{/each}
		{/if}
	</TableBody>
</Table>

<Separator fullWidthExtend={true} />

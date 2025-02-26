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
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import ActionButtons from '$lib/components/ActionButtons.svelte';
	import TrueFalseBadge from '$lib/components/TrueFalseBadge.svelte';

	const {
		title,
		items,
		basePath
	}: {
		title: string;
		items: (IJoinArgs | IGroupByArgs | IModelArgs | IStagingQueryArgs)[];
		basePath: string;
	} = $props();
</script>

<PageHeader {title} />

<div class="w-full">
	<ActionButtons class="mb-4" />
</div>

<Separator fullWidthExtend={true} />

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

<Separator fullWidthExtend={true} />

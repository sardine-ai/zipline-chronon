<script lang="ts">
	import type { Join } from '$lib/types/Model/Model';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import PageHeader from '$lib/components/PageHeader/PageHeader.svelte';
	import ActionButtons from '$lib/components/ActionButtons/ActionButtons.svelte';

	const { data } = $props();
	const joins: Join[] = $state(data.joins.items);

	// todo: remove this once we have data for all joins
	const reorderedJoins = [
		...joins.filter((join) => join.name === 'risk.user_transactions.txn_join'),
		...joins.filter((join) => join.name !== 'risk.user_transactions.txn_join')
	];
</script>

<PageHeader title="Joins"></PageHeader>

<div class="w-full">
	<ActionButtons class="mb-4" />
</div>
<Separator fullWidthExtend={true} />
<Table>
	<TableHeader>
		<TableRow>
			<TableHead>Join</TableHead>
		</TableRow>
	</TableHeader>
	<TableBody>
		{#each reorderedJoins as join}
			<TableRow>
				<TableCell>
					<a
						href={'/joins/' + encodeURIComponent(join.name)}
						class="hover:underline {join.name !== 'risk.user_transactions.txn_join'
							? 'pointer-events-none opacity-50'
							: ''}"
					>
						{join.name}
					</a>
				</TableCell>
			</TableRow>
		{/each}
	</TableBody>
</Table>
<Separator fullWidthExtend={true} />

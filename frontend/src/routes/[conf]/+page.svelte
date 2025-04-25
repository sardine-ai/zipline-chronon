<script lang="ts">
	import { Table, TextField } from 'svelte-ux';
	import { sort } from '@layerstack/utils';
	import { tableCell } from '@layerstack/svelte-table';
	import { cls } from '@layerstack/tailwind';

	import { afterNavigate } from '$app/navigation';
	import { page } from '$app/state';

	import PageHeader from '$lib/components/PageHeader.svelte';
	import Separator from '$lib/components/Separator.svelte';
	import { getEntityConfigFromPath } from '$lib/types/Entity';
	import TrueFalseBadge from '$src/lib/components/TrueFalseBadge.svelte';

	const { data } = $props();
	let searchTerm = $state('');

	// Clear search term after navigation
	afterNavigate(() => {
		searchTerm = '';
	});

	const sortedAndFilteredItems = $derived.by(() => {
		const term = searchTerm?.toLowerCase();

		const filtered = searchTerm
			? data.items.filter(
					(item) =>
						item.metaData?.name?.toLowerCase().includes(term) ||
						item.metaData?.team?.toLowerCase().includes(term)
				)
			: data.items;

		// TODO: remove this once we have observability data for all joins
		return sort(filtered, (d) => (d.metaData?.name === 'risk.user_transactions.txn_join' ? -1 : 1));
	});

	const entityConfig = $derived(getEntityConfigFromPath(page.url.pathname));
	const learnHref = $derived(data.items?.[0] ? entityConfig?.learnHref : 'https://chronon.ai/');
</script>

{#if entityConfig}
	<PageHeader title={entityConfig.label} {learnHref} />
	<TextField
		placeholder="Filter {entityConfig.label}..."
		classes={{
			root: 'max-w-xs mb-3 [--color:unset]',
			container: 'bg-transparent'
		}}
		bind:value={searchTerm}
	/>
	<Separator fullWidthExtend={true} />

	<Table
		data={sortedAndFilteredItems}
		columns={[
			{
				name: 'metaData.name',
				header: 'Name'
			},
			{
				name: 'metaData.team',
				header: 'Team'
			},
			{
				name: 'metaData.online',
				header: 'Online'
			},
			{
				name: 'metaData.production',
				header: 'Production'
			}
		]}
		classes={{
			table: 'text-sm',
			th: 'border-r border-b p-3 text-muted-foreground'
		}}
	>
		<tbody slot="data" let:columns let:data let:getCellValue let:getCellContent>
			{#each data ?? [] as rowData, rowIndex}
				<tr class="hover:bg-muted/50 border-b">
					{#each columns as column (column.name)}
						{@const value = getCellValue(column, rowData, rowIndex)}

						<td use:tableCell={{ column, rowData, rowIndex, tableData: data }} class="border-r p-3">
							{#if column.name === 'metaData.name'}
								<a
									href="{page.url.pathname}/{encodeURIComponent(value ?? '')}"
									class={cls(
										'hover:underline',
										// TODO: remove once we have observability data for all joins
										value !== 'risk.user_transactions.txn_join' && 'opacity-50'
									)}
								>
									{value}
								</a>
							{:else if ['metaData.online', 'metaData.production'].includes(column.name)}
								<TrueFalseBadge {value} />
							{:else}
								{getCellContent(column, rowData, rowIndex)}
							{/if}
						</td>{/each}
				</tr>
			{:else}
				<tr>
					<td colspan="4" class="p-3">No {entityConfig.label.toLowerCase()} found.</td>
				</tr>
			{/each}
		</tbody>
	</Table>
{:else}
	<PageHeader title="Unknown entity type" />
{/if}

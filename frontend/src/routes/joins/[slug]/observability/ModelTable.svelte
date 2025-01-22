<script lang="ts">
	import type { Model } from '$lib/types/Model/Model';
	import TrueFalseBadge from '$src/lib/components/TrueFalseBadge.svelte';
	import MetadataTable from '$lib/components/MetadataTable/MetadataTable.svelte';
	import MetadataTableSection from '$lib/components/MetadataTable/MetadataTableSection.svelte';
	import MetadataTableRow from '$lib/components/MetadataTable/MetadataTableRow.svelte';

	const { model }: { model: Model } = $props();
</script>

<MetadataTable columns={1} class="my-6">
	<MetadataTableSection>
		<MetadataTableRow label="Team" value={model.team} />
		<MetadataTableRow label="Model Type" value={model.modelType} />
		<MetadataTableRow label="Production">
			<TrueFalseBadge isTrue={model.production} />
		</MetadataTableRow>
	</MetadataTableSection>
</MetadataTable>

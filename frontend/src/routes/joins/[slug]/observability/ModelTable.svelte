<script lang="ts">
	import { ModelType, type IModel } from '$lib/types/codegen';
	import TrueFalseBadge from '$src/lib/components/TrueFalseBadge.svelte';
	import MetadataTable from '$lib/components/MetadataTable/MetadataTable.svelte';
	import MetadataTableSection from '$lib/components/MetadataTable/MetadataTableSection.svelte';
	import MetadataTableRow from '$lib/components/MetadataTable/MetadataTableRow.svelte';

	const { model }: { model: IModel } = $props();
</script>

<MetadataTable columns={1} class="my-6">
	<MetadataTableSection>
		<MetadataTableRow label="Team" value={model.metaData?.team} />
		<MetadataTableRow
			label="Model Type"
			value={model.modelType === undefined ? undefined : ModelType[model.modelType]}
		/>
		<MetadataTableRow label="Production">
			<TrueFalseBadge isTrue={model.metaData?.production ?? false} />
		</MetadataTableRow>
	</MetadataTableSection>
</MetadataTable>

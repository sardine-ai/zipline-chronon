<script lang="ts">
	import type { Model } from '$lib/types/Model/Model';
	import {
		Table,
		TableBody,
		TableCell,
		TableHead,
		TableHeader,
		TableRow
	} from '$lib/components/ui/table';

	const { data } = $props();
	const models: Model[] = $state(data.models.items);
</script>

<Table>
	<caption class="caption-top text-lg font-semibold text-left"> Models </caption>
	<TableHeader>
		<TableRow>
			<TableHead>Name</TableHead>
			<TableHead>Team</TableHead>
		</TableRow>
	</TableHeader>
	<TableBody>
		{#each models as model}
			<TableRow>
				<TableCell>
					<a href="/models/{model.id}/observability" class="hover:underline">
						{model.name}
					</a>
				</TableCell>
				<TableCell>{model.team}</TableCell>
			</TableRow>
		{/each}
	</TableBody>
</Table>

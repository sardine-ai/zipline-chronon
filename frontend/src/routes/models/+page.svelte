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
	import { Badge } from '$lib/components/ui/badge';

	const { data } = $props();
	const models: Model[] = $state(data.models.items);
</script>

<Table>
	<caption class="caption-top text-lg font-semibold text-left"> Models </caption>
	<TableHeader>
		<TableRow>
			<TableHead>Name</TableHead>
			<TableHead>Team</TableHead>
			<TableHead>Type</TableHead>
			<TableHead>Online</TableHead>
			<TableHead>Production</TableHead>
		</TableRow>
	</TableHeader>
	<TableBody>
		{#each models as model}
			<TableRow>
				<TableCell>
					<a href="/models/{model.name}/observability" class="hover:underline">
						{model.name}
					</a>
				</TableCell>
				<TableCell>{model.team}</TableCell>
				<TableCell>{model.modelType}</TableCell>
				<TableCell>
					{#if model.online}
						<Badge variant="success">True</Badge>
					{:else}
						False
					{/if}
				</TableCell>
				<TableCell>
					{#if model.production}
						<Badge variant="success">True</Badge>
					{:else}
						False
					{/if}
				</TableCell>
			</TableRow>
		{/each}
	</TableBody>
</Table>

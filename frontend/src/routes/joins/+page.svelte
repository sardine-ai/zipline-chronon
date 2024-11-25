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
	import TrueFalseBadge from '$lib/components/TrueFalseBadge/TrueFalseBadge.svelte';
	import Separator from '$lib/components/ui/separator/separator.svelte';
	import PageHeader from '$lib/components/PageHeader/PageHeader.svelte';
	import ActionButtons from '$lib/components/ActionButtons/ActionButtons.svelte';

	const { data } = $props();
	const models: Model[] = $state(data.models.items);
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
			<TableHead>Model</TableHead>
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
					<a href={'/joins/' + encodeURIComponent(model.join.name)} class="hover:underline">
						{model.join.name}
					</a>
				</TableCell>
				<TableCell>
					{model.name}
				</TableCell>
				<TableCell>{model.team}</TableCell>
				<TableCell>{model.modelType}</TableCell>
				<TableCell>
					<TrueFalseBadge isTrue={model.online} />
				</TableCell>
				<TableCell>
					<TrueFalseBadge isTrue={model.production} />
				</TableCell>
			</TableRow>
		{/each}
	</TableBody>
</Table>
<Separator fullWidthExtend={true} />

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
	import { Button } from '$lib/components/ui/button';
	import { Icon, Plus, ArrowsUpDown } from 'svelte-hero-icons';

	import Separator from '$lib/components/ui/separator/separator.svelte';
	import PageHeader from '$lib/components/PageHeader/PageHeader.svelte';

	const { data } = $props();
	const models: Model[] = $state(data.models.items);
</script>

<PageHeader title="Models"></PageHeader>

<div class="w-full">
	<div class="flex space-x-3 mb-4">
		<Button variant="secondary" size="sm">
			<Icon src={Plus} micro size="16" class="mr-2" />
			Filter
		</Button>
		<Button variant="secondary" size="sm">
			<Icon src={ArrowsUpDown} micro size="16" class="mr-2" />

			Sort
		</Button>
	</div>
</div>
<Separator fullWidthExtend={true} />
<Table>
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

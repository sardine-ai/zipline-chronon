<script lang="ts">
	import { Table, TableBody, TableCell, TableRow } from '$lib/components/ui/table/index.js';
	import type { Model } from '$lib/types/Model/Model';
	import TrueFalseBadge from '$src/lib/components/TrueFalseBadge.svelte';

	const { model }: { model: Model } = $props();
</script>

<div class="border rounded-md w-1/2 my-6">
	<Table density="compact">
		<TableBody>
			<TableRow>
				<TableCell align="left">Team</TableCell>
				<TableCell align="right">{model.team}</TableCell>
			</TableRow>
			<TableRow>
				<TableCell align="left">Model Type</TableCell>
				<TableCell align="right">{model.modelType}</TableCell>
			</TableRow>
			<TableRow>
				<TableCell align="left">Production</TableCell>
				<TableCell align="right">
					<TrueFalseBadge isTrue={model.production} />
				</TableCell>
			</TableRow>
		</TableBody>
	</Table>
</div>

<script lang="ts">
	import { Tabs, TabsList, TabsTrigger } from '$lib/components/ui/tabs';
	import IconTableCells from '~icons/heroicons/table-cells-16-solid';
	import IconChartLine from '~icons/zipline-ai/chart-line';
	import Separator from '$lib/components/ui/separator/separator.svelte';
</script>

<Tabs class="w-full">
	<TabsList>
		<TabsTrigger href="./drift" class="flex items-center">
			<IconChartLine class="mr-2 size-4" />
			Drift
		</TabsTrigger>
		<TabsTrigger href="./summary" class="flex items-center">
			<IconTableCells class="mr-2" />
			Summary
		</TabsTrigger>
	</TabsList>
	<Separator fullWidthExtend={true} />
</Tabs>

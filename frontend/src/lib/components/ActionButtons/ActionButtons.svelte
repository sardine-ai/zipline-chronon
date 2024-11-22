<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { cn } from '$lib/utils';
	import { Icon, Plus, ArrowsUpDown, Square3Stack3d, XMark } from 'svelte-hero-icons';

	let { showCluster = false, class: className }: { showCluster?: boolean; class?: string } =
		$props();

	let activeCluster = showCluster ? 'GroupBys' : null;
</script>

<div class={cn('flex flex-wrap gap-3', className)}>
	<!-- Active Items Section -->
	{#if activeCluster}
		<div class="flex flex-wrap gap-[1px]">
			<Button variant="secondaryAlt" size="sm" icon="leading" disabled>
				<Icon src={Square3Stack3d} micro size="16" />
				Cluster by
			</Button>
			<Button variant="secondaryAlt" size="sm" disabled>
				{activeCluster}
			</Button>
			<Button variant="secondaryAlt" size="sm" class="p-2" disabled>
				<Icon src={XMark} micro size="16" />
			</Button>
		</div>
	{/if}

	<!-- Action Buttons Section -->
	<div class="flex gap-3">
		<Button variant="secondary" size="sm" icon="leading">
			<Icon src={Plus} micro size="16" />
			Filter
		</Button>
		<Button variant="secondary" size="sm" icon="leading">
			<Icon src={ArrowsUpDown} micro size="16" />
			Sort
		</Button>
		{#if showCluster}
			<Button variant="secondary" size="sm" icon="leading">
				<Icon src={Square3Stack3d} micro size="16" />
				Cluster
			</Button>
		{/if}
	</div>
</div>

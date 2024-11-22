<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Icon, ArrowTrendingUp } from 'svelte-hero-icons';
	import { ChartSkew } from '@zipline-ai/icons';
	let {
		selected = $bindable('drift')
	}: {
		selected: string;
	} = $props();

	function toggle(value: 'drift' | 'skew') {
		selected = value;
	}
</script>

<div class="flex space-x-[1px]">
	<Button
		variant={selected === 'drift' ? 'default' : 'secondary'}
		size="sm"
		on:click={() => toggle('drift')}
		class="rounded-r-none"
		icon="leading"
	>
		<Icon src={ArrowTrendingUp} micro size="16" />
		Drift
	</Button>
	<Button
		variant={selected === 'skew' ? 'default' : 'secondary'}
		size="sm"
		on:click={() => toggle('skew')}
		class="rounded-l-none"
		icon="leading"
	>
		<Icon src={ChartSkew} size="16" />
		Skew
	</Button>
</div>

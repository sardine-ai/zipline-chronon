<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import { Icon, ArrowTrendingUp, ArrowsRightLeft } from 'svelte-hero-icons';

	let {
		selected = $bindable('drift')
	}: {
		selected: string;
	} = $props();

	function toggle(value: 'drift' | 'skew') {
		selected = value;
	}
</script>

<div class="flex space-x-2">
	<Button
		variant={selected === 'drift' ? 'default' : 'outline'}
		size="sm"
		on:click={() => toggle('drift')}
	>
		<Icon src={ArrowTrendingUp} micro size="16" class="mr-2" />
		Drift
	</Button>
	<Button
		variant={selected === 'skew' ? 'default' : 'outline'}
		size="sm"
		on:click={() => toggle('skew')}
	>
		<Icon src={ArrowsRightLeft} micro size="16" class="mr-2" />
		Skew
	</Button>
</div>

<script lang="ts">
	import { CopyButton } from 'svelte-ux';
	import { Inspect } from 'svelte-inspect-value';

	let { data } = $props();
</script>

<div>
	<CopyButton value={JSON.stringify(data.lineageTasks, null, 2)} />
</div>

<Inspect value={data.lineageTasks} />

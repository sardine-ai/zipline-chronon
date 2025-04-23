<script lang="ts">
	import { cls } from '@layerstack/tailwind';

	let {
		value
	}: {
		value: boolean | null | undefined;
	} = $props();
</script>

<span
	class={cls(
		'inline-block w-12 text-xs text-center font-medium py-0.5 border rounded-md',
		value ? 'bg-success-50 text-success-500 border-transparent' : 'border-neutral-500'
	)}
>
	{value ? 'True' : 'False'}
</span>

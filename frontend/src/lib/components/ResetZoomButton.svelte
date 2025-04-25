<script lang="ts">
	import { type ComponentProps } from 'svelte';
	import { Button as ButtonUX } from 'svelte-ux';
	import { cls } from '@layerstack/tailwind';

	import IconArrowUturnLeft from '~icons/heroicons/arrow-uturn-left-16-solid';

	const { onClick, ...restProps }: { onClick: () => void } & ComponentProps<ButtonUX> = $props();
</script>

<ButtonUX
	on:click={onClick}
	class={cls(
		'border border-surface-content/10 rounded-md text-xs _text-surface-content/70',
		restProps.class
	)}
>
	<IconArrowUturnLeft />
	Reset Zoom
</ButtonUX>

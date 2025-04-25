<script lang="ts">
	import type { HTMLAttributes } from 'svelte/elements';
	import { cls } from '@layerstack/tailwind';

	let {
		fullWidthExtend = false,
		wide = false,
		...restProps
	}: {
		fullWidthExtend?: boolean;
		wide?: boolean;
	} & HTMLAttributes<HTMLDivElement> = $props();
</script>

<div
	class={cls(
		'bg-border shrink-0',
		'h-[1px] w-full',
		fullWidthExtend && 'relative w-[calc(100%+4rem)] -mx-8',
		wide && 'h-[6px] bg-background border-y border-border',
		restProps.class
	)}
></div>

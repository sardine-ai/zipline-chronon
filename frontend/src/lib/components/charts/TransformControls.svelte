<script lang="ts">
	import type { SvelteHTMLElements } from 'svelte/elements';
	import { getTransformContext } from 'layerchart';
	import { Button, Tooltip } from 'svelte-ux';

	import IconArrowUturnLeft from '~icons/heroicons/arrow-uturn-left';
	import IconMagnifyingGlassPlus from '~icons/heroicons/magnifying-glass-plus';
	import IconMagnifyingGlassMinus from '~icons/heroicons/magnifying-glass-minus';

	import { cls } from '@layerstack/tailwind';

	type Placement =
		| 'top-left'
		| 'top'
		| 'top-right'
		| 'left'
		| 'center'
		| 'right'
		| 'bottom-left'
		| 'bottom'
		| 'bottom-right';

	type Actions = 'zoomIn' | 'zoomOut' | 'center' | 'reset' | 'scrollMode';
	interface Props {
		placement?: Placement;
		orientation?: 'horizontal' | 'vertical';
		size?: SvelteHTMLElements['svg']['width'];
		show?: Actions[];
		class?: string;
	}

	let {
		placement = 'top-right',
		orientation = 'vertical',
		size = '16',
		show = ['zoomIn', 'zoomOut', 'center', 'reset', 'scrollMode'],
		...restProps
	}: Props = $props();

	const transform = getTransformContext();
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	class={cls(
		'bg-surface-100/50 border rounded-full m-1 backdrop-blur-sm z-10 flex p-1',
		orientation === 'vertical' && 'flex-col',
		{
			'top-left': 'absolute top-0 left-0',
			top: 'absolute top-0 left-1/2 -translate-x-1/2',
			'top-right': 'absolute top-0 right-0',
			left: 'absolute top-1/2 left-0 -translate-y-1/2',
			center: 'absolute top-1/2 left-1/2 -translate-x-1/2  -translate-y-1/2',
			right: 'absolute top-1/2 right-0 -translate-y-1/2',
			'bottom-left': 'absolute bottom-0 left-0',
			bottom: 'absolute bottom-0 left-1/2 -translate-x-1/2',
			'bottom-right': 'absolute bottom-0 right-0'
		}[placement],
		restProps.class
	)}
	ondblclick={(e) => {
		// Stop from propagating to TransformContext
		e.stopPropagation();
	}}
>
	{#if show.includes('zoomIn')}
		<Tooltip title="Zoom in" placement="left" offset={2}>
			<Button on:click={() => transform.zoomIn()} iconOnly>
				<IconMagnifyingGlassPlus width={size} height={size} />
			</Button>
		</Tooltip>
	{/if}

	{#if show.includes('zoomOut')}
		<Tooltip title="Zoom out" placement="left" offset={2}>
			<Button on:click={() => transform.zoomOut()} iconOnly>
				<IconMagnifyingGlassMinus width={size} height={size} />
			</Button>
		</Tooltip>
	{/if}

	{#if show.includes('reset')}
		<Tooltip title="Reset" placement="left" offset={2}>
			<Button on:click={() => transform.reset()} iconOnly>
				<IconArrowUturnLeft width={size} height={size} />
			</Button>
		</Tooltip>
	{/if}
</div>
